const fs = require('fs');
const fsExtra = require('fs-extra');
const path = require('path');
const os = require('os');
const { google } = require('googleapis');
const puppeteer = require('puppeteer-core');
const proxyChain = require('proxy-chain');
const { setTimeout } = require('timers/promises');

// ==================== НАСТРОЙКИ ====================
const CONFIG = {
  // Эти параметры будут меняться динамически из листа "settings"
  maxConcurrency: 1,       // Количество потоков
  maxRetries: 3,           // Максимальное количество попыток на строку

  // Параметры Google Sheets
  spreadsheetId: '1rINK6ZEpVqP6wd_qOI1qMFtjY0j9B5TuArLn82wmf4k',
  // Лист с данными, которые парсим
  sheetName: 'default',
  // Лист, где лежат настройки
  settingsSheetName: 'settings',

  // Путь к JSON с учётными данными сервисного аккаунта
  credentialsPath: path.join(__dirname, 'observ-454800-148439f0838b.json'),

  // Путь к браузеру Chromium
  executablePath: '/usr/bin/chromium-browser',

  timeouts: {
    pageLoad: 60000,
    elementWait: 60000
  },

  // Пауза между обработкой строк (мс)
  perRowDelay: 1000,

  // Папка, где хранить временные профили Puppeteer
  userDataDirBase: '/home/ubuntu/puppeteer_profiles',

  // Интервал (мс) между циклами проверки "Number of required checks"
  checkIntervalMs: 10000
};

// ================ Служебные функции логирования ================
const logger = {
  info: (msg, ...params) => console.log(`[INFO] ${msg}`, ...params),
  error: (msg, ...params) => console.error(`[ERROR] ${msg}`, ...params),
  success: (msg, ...params) => console.log(`[SUCCESS] ${msg}`, ...params),
  debug: (msg, ...params) => console.log(`[DEBUG] ${msg}`, ...params)
};

// ==================== РАБОТА С ПРОКСИ ====================
async function setupProxy(proxyUrl) {
  if (!proxyUrl) return null;

  // Добавляем префикс socks5://, если не указан http/https/socks5
  if (
    !proxyUrl.startsWith('http://') &&
    !proxyUrl.startsWith('https://') &&
    !proxyUrl.startsWith('socks5://')
  ) {
    proxyUrl = `socks5://${proxyUrl}`;
  }

  try {
    return await proxyChain.anonymizeProxy(proxyUrl);
  } catch (err) {
    logger.error('Ошибка анонимизации прокси:', err.message);
    throw new Error(`Ошибка анонимизации прокси: ${err.message}`);
  }
}

// ==================== ЗАПУСК БРАУЗЕРА ====================
async function launchBrowser(proxyUrl = null) {
  fsExtra.ensureDirSync(CONFIG.userDataDirBase);

  const tempProfileDir = path.join(
    CONFIG.userDataDirBase,
    `puppeteer_temp_${Date.now()}_${Math.random().toString(16).slice(2)}`
  );

  const launchOptions = {
    headless: true,
    executablePath: CONFIG.executablePath,
    userDataDir: tempProfileDir,
    args: [
      '--no-sandbox',
      '--disable-setuid-sandbox',
      '--disable-dev-shm-usage',
      '--disable-accelerated-2d-canvas',
      '--no-first-run',
      '--no-zygote',
      '--disable-gpu'
    ]
  };

  if (proxyUrl) {
    launchOptions.args.push(`--proxy-server=${proxyUrl}`);
  }

  const browser = await puppeteer.launch(launchOptions);

  // Сохраняем путь к временному профилю, чтобы потом удалить
  browser._tempProfileDir = tempProfileDir;

  return browser;
}

// ==================== ИЗВЛЕЧЕНИЕ ДАННЫХ СО СТРАНИЦЫ ====================
async function extractData(page, url) {
  try {
    await page.goto(url, {
      waitUntil: 'networkidle2',
      timeout: CONFIG.timeouts.pageLoad
    });
    await page.waitForSelector('[data-testid="pro-list-result"]', {
      timeout: CONFIG.timeouts.elementWait
    });

    return await page.evaluate(() => {
      const resultBlocks = document.querySelectorAll('[data-testid="pro-list-result"]');
      return [...resultBlocks].map((block) => {
        // Извлекаем название
        const nameEl = block.querySelector('.Z4pkQxpLz9qpVDR7jJHjW');
        const name = nameEl ? nameEl.textContent.trim() : '';

        // Извлекаем рейтинг и количество отзывов
        const ratingEl = block.querySelector(
          '[data-testid="pro-list-result-ratings"] ._3iW9xguFAEzNAGlyAo5Hw7'
        );
        const ratingText = ratingEl ? ratingEl.textContent.trim() : '';
        let rating = '';
        let reviewsCount = '';
        if (ratingText) {
          const match = ratingText.match(/(\d+(\.\d+)?)\s*\((\d+)\)/);
          if (match) {
            rating = match[1];
            reviewsCount = match[3];
          } else if (ratingText.match(/\d+(\.\d+)?/)) {
            rating = ratingText.match(/\d+(\.\d+)?/)[0];
          } else {
            rating = ratingText;
          }
        }

        // Извлекаем цену
        let price = '';
        const priceBlock = block.querySelector('p._1DeCbTZuso1A63BkkD1Z8Q span.b.black');
        if (priceBlock) {
          price = priceBlock.textContent.trim();
        } else {
          const altPriceEl = block.querySelector('p._178AiGzmuR43MQQ1DfV4B9');
          price = altPriceEl ? altPriceEl.textContent.trim() : '';
        }

        return { name, rating, reviewsCount, price };
      });
    });
  } catch (error) {
    logger.error('Ошибка при извлечении данных:', error.message);
    throw error;
  }
}

// ==================== ОБНОВЛЕНИЕ ОТВЕТА В SHEETS (колонки E:H) ====================
async function updateSpreadsheet(sheets, rowIndex, values, retryCount = 0) {
  try {
    await sheets.spreadsheets.values.update({
      spreadsheetId: CONFIG.spreadsheetId,
      range: `${CONFIG.sheetName}!E${rowIndex}:H${rowIndex}`, // E:H
      valueInputOption: 'USER_ENTERED',
      requestBody: { values: [values] }
    });
    return true;
  } catch (error) {
    logger.error(`Ошибка обновления строки ${rowIndex}:`, error.message);

    // Пример "бэкоффа" при ошибке квоты
    if (
      error.message &&
      error.message.includes('Quota exceeded for quota metric')
    ) {
      if (retryCount < 5) {
        const delayMs = 2000 * Math.pow(2, retryCount); // 2с -> 4с -> 8с -> ...
        logger.info(
          `Перепробуем update строки #${rowIndex} через ${delayMs} мс (попытка ${
            retryCount + 1
          })...`
        );
        await setTimeout(delayMs);
        return updateSpreadsheet(sheets, rowIndex, values, retryCount + 1);
      } else {
        logger.error(`Достигнут лимит попыток бэкоффа для строки #${rowIndex}.`);
      }
    }
    return false;
  }
}

// ==================== ОБРАБОТКА ОДНОЙ СТРОКИ ====================
async function processRow(rowData, rowIndex, sheets) {
  let [orgName, zip, oldProxyUrl, url] = rowData;
  let newProxyUrl = null;
  let browser = null;

  logger.info(`\n--- Обработка строки #${rowIndex} ---`);
  logger.info(`Organization name: ${orgName}`);
  logger.info(`ZIP: ${zip}`);
  logger.info(`Proxy: ${oldProxyUrl}`);
  logger.info(`URL: ${url}`);

  if (!url) {
    logger.info(`Строка ${rowIndex}: нет URL, пропускаем.`);
    return false;
  }

  try {
    // Настраиваем прокси, если он есть
    if (oldProxyUrl) {
      newProxyUrl = await setupProxy(oldProxyUrl);
      logger.info('Анонимизированная прокси:', newProxyUrl);
    }

    // Несколько попыток парсинга
    for (let attempt = 1; attempt <= CONFIG.maxRetries; attempt++) {
      logger.info(`Попытка ${attempt} для строки #${rowIndex}`);

      try {
        browser = await launchBrowser(newProxyUrl);
        const page = await browser.newPage();

        // Устанавливаем User-Agent
        await page.setUserAgent(
          'Mozilla/5.0 (Windows NT 10.0; Win64; x64) ' +
            'AppleWebKit/537.36 (KHTML, like Gecko) ' +
            'Chrome/115.0.0.0 Safari/537.36'
        );
        await page.setViewport({ width: 1280, height: 800 });

        // Извлекаем данные
        const data = await extractData(page, url);
        logger.success(`Парсинг завершён. Всего результатов: ${data.length}`);

        // Определяем позицию организации
        let positionInRanking = 'Not found';
        const foundIndex = data.findIndex(
          item => item.name.toLowerCase() === orgName.toLowerCase()
        );
        if (foundIndex >= 0) {
          positionInRanking = (foundIndex + 1).toString();
        }

        const totalQuantity = data.length.toString();
        const responseJson = JSON.stringify(data);
        const recordingDate = new Date().toISOString();

        // Обновляем в Sheets (колонки E, F, G, H)
        await updateSpreadsheet(sheets, rowIndex, [
          positionInRanking,
          totalQuantity,
          responseJson,
          recordingDate
        ]);

        logger.success(
          `Строка #${rowIndex} обновлена: Position=${positionInRanking}, Total=${totalQuantity}`
        );

        // Закрываем браузер
        await browser.close();
        if (browser._tempProfileDir) {
          await fsExtra.remove(browser._tempProfileDir).catch(() => {});
        }

        // Если всё ок — выходим из цикла
        return true;
      } catch (err) {
        logger.error(`Попытка ${attempt} не удалась:`, err.message);

        // Закрываем браузер, если был
        if (browser) {
          try {
            await browser.close();
          } catch (_) {}
          if (browser._tempProfileDir) {
            await fsExtra.remove(browser._tempProfileDir).catch(() => {});
          }
        }

        // Если последняя попытка — пишем ошибку в Sheets
        if (attempt === CONFIG.maxRetries) {
          await updateSpreadsheet(sheets, rowIndex, [
            'Not processed',
            '0',
            `Ошибка: ${err.message}`,
            new Date().toISOString()
          ]);
        }
      }
    }
  } catch (error) {
    logger.error(
      `Обработка строки #${rowIndex} завершилась с ошибкой:`,
      error.message
    );
    await updateSpreadsheet(sheets, rowIndex, [
      'Not processed',
      '0',
      `Ошибка: ${error.message}`,
      new Date().toISOString()
    ]);
  } finally {
    // Закрываем прокси, если был
    if (newProxyUrl) {
      await proxyChain.closeAnonymizedProxy(newProxyUrl).catch(() => {});
    }
  }

  return false;
}

// ==================== ПУЛ ЗАДАЧ (с динамическим concurrency) ====================
class TaskQueue {
  constructor(concurrency, afterTaskCallback) {
    this.concurrency = concurrency;
    this.running = 0;
    this.queue = [];
    this.afterTaskCallback = afterTaskCallback;
  }

  async add(fn) {
    return new Promise((resolve, reject) => {
      this.queue.push({ fn, resolve, reject });
      this.runNext();
    });
  }

  async runNext() {
    if (this.running >= this.concurrency || this.queue.length === 0) {
      return;
    }
    this.running++;
    const { fn, resolve, reject } = this.queue.shift();
    try {
      const result = await fn();
      resolve(result);
      // После выполнения задачи вызываем callback
      if (this.afterTaskCallback) {
        await this.afterTaskCallback();
      }
    } catch (error) {
      reject(error);
    } finally {
      this.running--;
      this.runNext();
    }
  }

  // Позволяет менять кол-во потоков "на лету"
  setConcurrency(newConcurrency) {
    this.concurrency = newConcurrency;
    // Если потоков стало больше — возможно, нужно взять задачи из очереди
    this.runNext();
  }
}

// =============================================================
// ==========  ФУНКЦИИ ДЛЯ ЧТЕНИЯ/ОБНОВЛЕНИЯ НАСТРОЕК  =========
// =============================================================

/**
 * Считываем настройки из листа "settings" (строка 2, столбцы A–E).
 * Предположим, что:
 *   A: Number of required checks
 *   B: Number of threads to run
 *   C: Last run Start
 *   D: Last run End
 *   E: Rows processed
 */
async function getSettings(sheets) {
  const range = `${CONFIG.settingsSheetName}!A2:E2`;
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: CONFIG.spreadsheetId,
    range
  });
  const values = res.data.values && res.data.values[0] ? res.data.values[0] : [];
  const [
    requiredChecks,
    concurrency,
    lastRunStart,
    lastRunEnd,
    rowsProcessed
  ] = values;

  return {
    requiredChecks: parseInt(requiredChecks, 10) || 0,
    concurrency: parseInt(concurrency, 10) || 1,
    lastRunStart: lastRunStart || '',
    lastRunEnd: lastRunEnd || '',
    rowsProcessed: parseInt(rowsProcessed, 10) || 0
  };
}

/**
 * Обновляем настройки (все сразу).
 */
async function updateSettings(sheets, {
  requiredChecks,
  concurrency,
  lastRunStart,
  lastRunEnd,
  rowsProcessed
}) {
  const range = `${CONFIG.settingsSheetName}!A2:E2`;
  const values = [[
    requiredChecks,
    concurrency,
    lastRunStart,
    lastRunEnd,
    rowsProcessed
  ]];
  await sheets.spreadsheets.values.update({
    spreadsheetId: CONFIG.spreadsheetId,
    range,
    valueInputOption: 'USER_ENTERED',
    requestBody: { values }
  });
}

// ===================================================================
// ==========  ФУНКЦИЯ, КОТОРАЯ ОБРАБАТЫВАЕТ ЛИСТ `default`  ==========
// ===================================================================
/**
 * Обрабатывает лист "default" и возвращает, сколько строк обработано.
 * Здесь же реализуем обновление "Rows processed" после каждой строки,
 * а также динамическое изменение количества потоков.
 */
async function runDefaultSheet(sheets) {
  // Считаем настройки (чтобы узнать актуальное concurrency)
  let currentSettings = await getSettings(sheets);
  // Установим в CONFIG, чтобы TaskQueue знал стартовое значение
  CONFIG.maxConcurrency = currentSettings.concurrency;

  // Читаем строки из основного листа (A2:D)
  const readRange = `${CONFIG.sheetName}!A2:D`;
  const getRes = await sheets.spreadsheets.values.get({
    spreadsheetId: CONFIG.spreadsheetId,
    range: readRange
  });
  const rows = getRes.data.values || [];

  if (!rows.length) {
    logger.info('Нет строк для обработки в листе default.');
    return 0;
  }

  logger.info(`Всего строк для обработки: ${rows.length}`);
  logger.info(`Начинаем в ${CONFIG.maxConcurrency} поток(а/ов)`);

  let rowsProcessed = 0;

  // Callback, вызываемый после каждой обработанной строки
  const afterTaskCallback = async () => {
    rowsProcessed++;
    // Обновим настройки: Rows processed + прочитаем заново concurrency
    let settingsNow = await getSettings(sheets);

    // Ставим новое значение "Rows processed"
    settingsNow.rowsProcessed = rowsProcessed;
    await updateSettings(sheets, settingsNow);

    // Если concurrency в таблице поменялся — обновим пул
    if (settingsNow.concurrency !== CONFIG.maxConcurrency) {
      CONFIG.maxConcurrency = settingsNow.concurrency;
      taskQueue.setConcurrency(settingsNow.concurrency);
      logger.info(`Concurrency изменён на ${settingsNow.concurrency} во время работы`);
    }
  };

  // Создаем пул задач
  const taskQueue = new TaskQueue(CONFIG.maxConcurrency, afterTaskCallback);
  const results = [];

  // Добавляем задачи на все строки
  for (let i = 0; i < rows.length; i++) {
    const rowIndex = i + 2; // т.к. начинаем с A2
    results.push(
      taskQueue.add(async () => {
        await processRow(rows[i], rowIndex, sheets);
        // Пауза между строками
        await setTimeout(CONFIG.perRowDelay);
      })
    );
  }

  // Ждем завершения всех задач
  await Promise.all(results);

  logger.success('Обработка листа default завершена!');
  return rowsProcessed;
}

// ===================================================================
// ==========  ГЛАВНЫЙ ЦИКЛ, КОТОРЫЙ СЛЕДИТ ЗА "SETTINGS"  ============
// ===================================================================
async function watchSettingsLoop(sheets) {
  while (true) {
    try {
      // Считываем настройки
      const settings = await getSettings(sheets);
      let { requiredChecks, concurrency } = settings;

      if (requiredChecks > 0) {
        logger.info(`Нужно сделать проверку. Number of required checks = ${requiredChecks}`);

        // Записываем время старта и сбрасываем "Rows processed"
        settings.lastRunStart = new Date().toISOString();
        settings.lastRunEnd = ''; // Пока пусто
        settings.rowsProcessed = 0;
        // Обновляем concurrency (вдруг поменяли)
        CONFIG.maxConcurrency = concurrency;

        // Запишем начальные настройки
        await updateSettings(sheets, settings);

        // Запускаем обработку листа default
        const rowsProcessed = await runDefaultSheet(sheets);

        // Обновляем настройки после выполнения: записываем время завершения и количество обработанных строк
        settings.lastRunEnd = new Date().toISOString();
        settings.rowsProcessed = rowsProcessed;

        // Уменьшаем requiredChecks на 1 (однократно)
        settings.requiredChecks = requiredChecks - 1;
        if (settings.requiredChecks < 0) settings.requiredChecks = 0;

        await updateSettings(sheets, settings);
      } else {
        logger.info('Number of required checks = 0. Ждем...');
      }
    } catch (err) {
      logger.error('Ошибка в watchSettingsLoop:', err.message);
    }

    // Ждем 10 секунд (или сколько вам нужно)
    await setTimeout(CONFIG.checkIntervalMs);
  }
}

// ===================================================================
// ==========   СТАРТ: АВТОРИЗАЦИЯ И ЗАПУСК ЦИКЛА ОЖИДАНИЯ    ==========
// ===================================================================
async function main() {
  try {
    // Авторизация в Google Sheets
    const credentials = JSON.parse(fs.readFileSync(CONFIG.credentialsPath, 'utf8'));
    const auth = new google.auth.JWT(
      credentials.client_email,
      null,
      credentials.private_key,
      ['https://www.googleapis.com/auth/spreadsheets']
    );
    const sheets = google.sheets({ version: 'v4', auth });

    // Запускаем «бесконечный» цикл слежения за настройками
    await watchSettingsLoop(sheets);
  } catch (err) {
    logger.error('Главная ошибка:', err);
    process.exit(1);
  }
}

main();
