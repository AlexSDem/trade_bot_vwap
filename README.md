# Intraday Trading Bot (T-Invest)

1. git clone https://github.com/AlexSDem/trade_bot
2. cd trade_bot
3. pip install -r requirements.txt
4. copy config.yaml.example config.yaml
5. python main.py

Простой внутридневной торговый бот для акций РФ.

**Особенности:**
- рынок: акции РФ (MOEX)
- брокер: T-Invest (Invest API)
- стратегия: intraday mean reversion (возврат к VWAP)
- режим: пулл минутных свечей (1m)
- лонг-онли, без плеча
- жёсткий риск-менеджмент и дневной лимит убытка
- предназначен для запуска локально (Windows / macOS / Linux)

---

## Структура проекта

```text
.
├── main.py                 # основной цикл
├── broker.py               # работа с Invest API (заявки, позиции, данные)
├── strategy.py             # логика входа/выхода (тейк/стоп/тайм-стоп)
├── risk.py                 # риск-менеджмент и дневные лимиты
├── state.py                # состояние позиций и заявок
├── config.yaml.example     # шаблон конфига (копировать в config.yaml)
├── requirements.txt
├── logs/
│   └── .gitkeep
└── README.md
```

## Быстрый старт (Windows)
1️⃣ Подготовка окружения

Установи Python 3.10+

Проверь:
```text
python --version
pip --version
```

2️⃣ Установка зависимостей
```text
pip install -r requirements.txt
```

3️⃣ Настройка API-токена

В PowerShell один раз:
```text
setx INVEST_TOKEN "ТВОЙ_API_ТОКЕН_T_INVEST"
```

Закрой PowerShell, открой заново и проверь:
```text
echo $env:INVEST_TOKEN
```

⚠️ Никогда не добавляй токен в код или конфиг.

4️⃣ Конфигурация

Скопируй шаблон:
```text
copy config.yaml.example config.yaml
```

По умолчанию:
- используется sandbox
- торговля начинается после открытия рынка
- позиции не переносятся через ночь

5️⃣ Запуск
```text
python main.py
```

Остановка:
```text
Ctrl + C
```

## Sandbox и боевой режим

По умолчанию в config.yaml:
```text
broker:
  use_sandbox: true
```

➡️ РЕКОМЕНДУЕТСЯ:
1. Прогнать бота 2–3 торговых дня в sandbox
2. Проверить логи (logs/bot.log)
3. Только потом переключать:
broker:
  use_sandbox: false

## Как работает стратегия (кратко)
- вход: отклонение цены вниз от VWAP на k * ATR
- выход:
    - тейк-профит
    - стоп-лосс
    - тайм-стоп
- только лимитные заявки
- не более заданного числа сделок в день
- дневной лимит убытка блокирует торговлю до следующего дня

## Логи и контроль
- все действия пишутся в logs/bot.log
- для штатной работы достаточно 15–20 минут контроля в день


# Пояснения по файлам
## broker.py
`Broker` — это слой между твоей стратегией/рисками и API Тинькофф Инвестиций. Он отвечает за роутинг sandbox/real, разрешение тикеров в инструменты (FIGI/лот/шаг цены), синхронизацию позиций и ордеров, выставление лимитных заявок и пуллинг их статуса, а также журналирование.

---

## 1) Роль класса `Broker`

`Broker` решает 5 задач:

1. **Роутинг sandbox/real**  
   Один и тот же код работает и в песочнице, и на реале: выбираются правильные методы клиента (`sandbox.*` vs `orders/operations.*`).

2. **Разрешение тикера → FIGI + справочник инструмента**  
   `resolve_instruments()` через `client.instruments.share_by()` получает:
   - `figi`
   - `lot`
   - `min_price_increment` (шаг цены)  
   и сохраняет в `self._figi_info[figi] = InstrumentInfo(...)`.

3. **Снимок аккаунта: деньги, позиции, активные ордера**  
   `refresh_account_snapshot()` читает:
   - деньги (money)
   - позиции (securities)
   - активные ордера  
   и синхронизирует это с локальным `BotState`.

4. **Постановка лимитных ордеров + простая идемпотентность**  
   При выставлении создаётся `client_uid = uuid4()` и передаётся в `order_id` (в API — idempotency key). Локально сохраняется:
   - `active_order_id` (реальный order_id от брокера)
   - `client_order_uid`
   - `order_side`
   - `order_placed_ts`

5. **Пуллинг статуса ордера и финализация**  
   `poll_order_updates()` периодически запрашивает `get_order_state()`. Если ордер финализировался (FILL/CANCEL/REJECT), пишет событие в журнал и чистит локальное состояние.

---

## 2) Локальное состояние (`state.py`)

На каждый FIGI хранится `FigiState`:

- `position_lots` — **в лотах**, не в штуках
- `active_order_id` — биржевой id ордера (из ответа API)
- `client_order_uid` — твой uuid (идемпотентность/логирование)
- `order_side`, `order_placed_ts` — для TTL и понимания “что висит”
- `entry_price`, `entry_time` — используются стратегией и для PnL-уведомлений

Также есть:
- `BotState.trades_today`
- `BotState.current_day` (для дневного rollover / отчёта)

---

## 3) Деньги: `cash`, `reserved`, `free ≈`

Есть два уровня учёта:

### A) Реальный кэш из позиций
`get_cash_rub()` читает `pos.money` и суммирует только `currency == self.currency` (обычно `"rub"`).

### B) Резерв под уже выставленные BUY-ордера
`self._reserved_rub_by_figi[figi] = est_cost`

Это **локальная оценка**, чтобы бот не выставлял несколько покупок подряд и не упирался в “недостаточно средств”.

Далее:
- `reserved_total = sum(_reserved_rub_by_figi.values())`
- `free ≈ cash - reserved_total`

**Используется в `place_limit_buy()`**: если `free_cash < est_cost * 1.01` → `SKIP` + лог + `journal`.

---

## 4) Конвертация позиции в лоты

Тинькофф в `positions.securities.balance` обычно даёт **кол-во бумаг**, но торгуешь ты **лотами**.

Поэтому:
- `lot = InstrumentInfo.lot`
- `_balance_to_lots(figi, balance)`:
  - если `lot <= 1` → lots = `int(balance)`
  - иначе lots = `floor(balance / lot)`

Важно: **везде дальше `position_lots` — именно лоты**, и при продаже `place_limit_sell_to_close()` передаёт `quantity=int(fs.position_lots)` — это корректно (API ждёт количество лотов).

---

## 5) “Агрессивная” цена лимитника около `last` (`_aggressive_near_last`)

Стратегия может сказать “suggested limit_price”, но брокер смещает цену ближе к рынку:

- **BUY**:  
  `target = last + buy_aggressive_ticks * step`  
  берёт `max(suggested_price, target)` и округляет **вверх** по шагу.

- **SELL**:  
  `target = last - sell_aggressive_ticks * step`  
  берёт `min(suggested_price, target)` и округляет **вниз** по шагу.

Параметры (из конфига):
- `buy_aggressive_ticks` (по умолчанию 1)
- `sell_aggressive_ticks` (по умолчанию 1)

---

## 6) Журнал (`TradeJournal`)

`journal_event()` — обёртка, которая автоматически подставляет `ticker` по FIGI.

Типовые события:
- `SUBMIT` — ордер отправлен в API
- `FILL` / `CANCEL` / `REJECT` — финальные статусы
- `EXPIRE` — TTL истёк → отмена
- `SKIP` — пред-проверка не прошла (например, не хватает free cash)
- `STATE_LOST` — `get_order_state` вернул NOT_FOUND (локальный ордер “потерян”)

Это “аудит-трейл”, на базе которого удобно строить дневной отчёт.

---

## 7) Жизненный цикл ордера в основном цикле (обычно `main.py`)

На каждый FIGI в каждой итерации:

1. `expire_stale_orders(ttl_sec)`  
   Если ордер висит дольше TTL → `EXPIRE` → `cancel_active_order()`.

2. `poll_order_updates()`  
   Если стал `FILL/CANCEL/REJECT` → журнал + уведомление + очистка state + снятие резерва.

3. Маркет-данные → стратегия даёт сигнал.

4. Если **BUY**:
   - проверка: нет позиции и нет активного ордера
   - precheck по `free cash` (учитывая `reserved`)
   - выставление лимитника (`SUBMIT`) и резервирование `est_cost`

5. Если **SELL**:
   - если уже висит ордер — отменяется и заменяется
   - ставится SELL на `position_lots`

---

## 8) `flatten_if_needed()` — закрыть всё по времени

Когда наступает `flatten_time`:
- отменяются активные ордера
- если есть позиции → ставится SELL close по текущему `last`

Задача: **не оставлять позиции на ночь** (если так настроено расписание).

---

## 9) Важные нюансы

- `last_cash_rub` кэшируется в `refresh_account_snapshot()`. Если долго не обновлять snapshot, precheck по cash может устареть — но при регулярном вызове snapshot это ок.
- Резерв денег снимается:
  - при отмене
  - при финальном статусе
  - при “потерянном” ордере (`STATE_LOST`)  
  Это важно, иначе бот мог бы навсегда “зажать” free cash.
- `calc_day_cashflow()` суммирует `op.payment` за день. Это скорее “итог платежей” (покупки/продажи/комиссии), а не чистый PnL, но как дневная метрика бывает полезно.

---
