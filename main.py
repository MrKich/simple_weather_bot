import telegram
import requests
import requests.exceptions
import logging
import datetime
import threading

from telegram.ext import CommandHandler, Updater, MessageHandler, Filters

from signal_exitter import SignalExitter
from db import DB


logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)


class WeatherBot:
    main_keyboard = [['/cweather'], ['/history']]
    reply_markup = telegram.ReplyKeyboardMarkup(main_keyboard)
    watch_cities = [('Москва', 524901), ('Санкт-Петербург', 498817), ('Ярославль', 468902)]

    def __init__(self, dbname, api_key, bot_token):
        self.db = DB(dbname)
        self.api_key = api_key
        self.bot_token = bot_token
        self.weather_api_format_weather = 'http://api.openweathermap.org/data/2.5/weather?lang=ru&units=metric&appid={}'.format(api_key)
        self.weather_api_format_find = 'http://api.openweathermap.org/data/2.5/find?type=accurate&lang=ru&units=metric&cnt=10&appid={}'.format(api_key)

        self.updater = Updater(token=self.bot_token)
        self.t = threading.Thread(target=self.collector_thread, args=(self.watch_cities,))
        self.watching_stop_event = threading.Event()
        self.last_cmd = {}
        self.last_cities = {}

        with self.db.cursor() as s:
            s.execute('CREATE TABLE IF NOT EXISTS `weather` ('
                      '`city_name`  TEXT NOT NULL,'
                      '`dt`    INTEGER NOT NULL,'
                      '`type`  TEXT NOT NULL,'
                      '`temperature`   REAL NOT NULL,'
                      '`wind_speed`    REAL NOT NULL);')

    def start(self):
        dispatcher = self.updater.dispatcher

        dispatcher.add_handler(CommandHandler('start', self.cmd_start))
        dispatcher.add_handler(CommandHandler('cweather', self.cmd_current_weather, pass_args=True, pass_job_queue=True))
        dispatcher.add_handler(CommandHandler('history', self.cmd_weather_history, pass_args=True, pass_job_queue=True))
        dispatcher.add_handler(MessageHandler(Filters.text, self.cmd_call_last_cmd, pass_job_queue=True))
        dispatcher.add_handler(MessageHandler(Filters.command, self.cmd_unknown_cmd))

        logging.info('Began polling')
        self.updater.start_polling()
        self.t.start()

    def stop(self):
        logging.info('Stopping bot...')
        self.watching_stop_event.set()
        self.t.join()
        self.updater.stop()

    def query_weather_online(self, q_city, city_id=None):
        if city_id is not None:
            q = '{}&id={}'.format(self.weather_api_format_weather, city_id)
        else:
            q = '{}&q={}'.format(self.weather_api_format_find, q_city)

        try:
            # print(q)
            data = requests.get(q, timeout=3).json()
            # print(data)
        except requests.exceptions.BaseHTTPError as e:
            logging.warn('Error accessing weather api: {}'.format(e))
        else:
            code = int(data['cod'])
            if code == 200:
                if city_id is not None:
                    return [data]
                else:
                    return data['list']

    def collect_weather_data(self, city, city_id=None):
        if city_id is not None:
            data = self.query_weather_online('', city_id)
        else:
            data = self.query_weather_online(city)

        if data is None:
            return False
        elif len(data) > 1:
            raise Exception('Multiple cities in watch_cities [{}]. Try id or another city.'.format(city))
        elif len(data) == 0:
            raise Exception('City from watch_cities is not found [{}]. Try another city.'.format(city))
        data = data[0]
        with self.db.cursor() as s:
            already_inserted = s.execute('SELECT * FROM weather WHERE city_name = ? and dt = ? LIMIT 1', (city, data['dt'])).fetchone()
            if already_inserted is None:
                s.execute('INSERT INTO weather VALUES(?, ?, ?, ?, ?)', (city, data['dt'], data['weather'][0]['description'],
                                                                        data['main']['temp'], data['wind']['speed']))
                logging.info('Updated data for {} at {}'.format(city, datetime.datetime.fromtimestamp(data['dt'])))
        return True

    def collector_thread(self, cities):
        while not self.watching_stop_event.wait(30):
            for city, city_id in self.watch_cities:
                with self.db.cursor() as s:
                    now = datetime.datetime.now()
                    # delta = datetime.timedelta(minutes=1)
                    delta = datetime.timedelta(hours=6)
                    data = s.execute('SELECT * FROM weather WHERE city_name = ? and dt > ? ORDER BY DT DESC LIMIT 1',
                                     (city, (now - delta).timestamp())).fetchone()
                    if data is not None:
                        continue
                    else:
                        res = self.collect_weather_data(city, city_id=city_id)
                        if not res:
                            logging.warn('Could not query information about city: {}'.format(city))
                            self.watching_stop_event.wait(1)

    def query_weather_callback(self, bot, update):
        chat_id, *args = update.context
        cities = self.query_weather_online(*args)

        if cities is None:
            text = ("Очень сожалею, но ответ погодного сервера мне не ясен =(\n"
                    "Попробуйте ещё раз, но чуть попозже")
            bot.send_message(chat_id=chat_id, text=text, reply_markup=self.reply_markup, parse_mode=telegram.ParseMode.HTML)
        elif len(cities) == 0:
            text = 'К сожалению, не нашёл вашего города. Попробуйте обозвать его как-нибудь по иному ;)'
            bot.send_message(chat_id=chat_id, text=text, reply_markup=self.reply_markup, parse_mode=telegram.ParseMode.HTML)
        elif len(cities) > 1:
            cities.sort(key=lambda x: '{}, {}'.format(x['name'], x['sys']['country']))
            self.last_cities[chat_id] = cities
            cities_by_name = ['{}, {}'.format(x['name'], x['sys']['country']) for x in cities]
            cities_str = '\n'.join(['<b>{}</b>[{}]'.format(x, i + 1) for i, x in enumerate(cities_by_name)])
            text = ("Обнаружил больше одного города, попробуйте уточнить запрос (показываю только 10):\n"
                    "{}".format(cities_str))
            bot.send_message(chat_id=chat_id, text=text, reply_markup=self.reply_markup, parse_mode=telegram.ParseMode.HTML)
        else:
            city = cities[0]
            text = ("В городе {}, {} на момент {}\n\n"
                    "Температура => <b>{} °C</b>\n"
                    "Скорость ветра => <b>{} м/с</b>\n"
                    "Тип погоды => <b>{}</b>").format(city['name'], city['sys']['country'], datetime.datetime.fromtimestamp(city['dt']),
                                                      city['main']['temp'], city['wind']['speed'],
                                                      city['weather'][0]['description'])
            bot.send_message(chat_id=chat_id, text=text, reply_markup=self.reply_markup, parse_mode=telegram.ParseMode.HTML)

    def cmd_start(self, bot, update):
        update.message.reply_text("Привет, я маленький погодный бот.\nМожешь попросить меня вот о чем:\n\n"
                                  "<b>/cweather</b> [город]\t-\tи я выведу текущую погоду в указанном городе\n"
                                  "<b>/history</b> [город]\t-\tи я покажу тебе информацию о погоде за прошлую неделю",
                                  reply_markup=self.reply_markup, parse_mode=telegram.ParseMode.HTML)

    def cmd_current_weather(self, bot, update, job_queue, args):
        self.last_cmd[update.message.chat_id] = self.cmd_current_weather
        if not args:
            update.message.reply_text("Хорошо, если сейчас напишите город, то я проверю погоду в нём.")
        else:
            self.last_cities[update.message.chat_id] = []
            job_queue.run_once(self.query_weather_callback, 0, context=(update.message.chat_id, ' '.join(args)))

    def cmd_weather_history(self, bot, update, job_queue, args):
        self.last_cities[update.message.chat_id] = []
        self.last_cmd[update.message.chat_id] = self.cmd_weather_history
        if not args:
            available_cities = ', '.join([x[0] for x in self.watch_cities])
            update.message.reply_text("Хорошо, если сейчас напишите город, то я выведу информацию о погоде за прошедшую неделю.\n\n"
                                      "Псссс, я пока умею только в следующие города:\n({})".format(available_cities),
                                      reply_markup=self.reply_markup, parse_mode=telegram.ParseMode.HTML)
        else:
            city = ' '.join(args)
            if city not in [x[0] for x in self.watch_cities]:
                update.message.reply_text('К сожалению, за этим городом я не наблюдаю. Попробуйте изменить мои настройки',
                                          reply_markup=self.reply_markup, parse_mode=telegram.ParseMode.HTML)
            else:
                with self.db.cursor() as s:
                    now = datetime.datetime.now()
                    delta = datetime.timedelta(days=7)
                    data = s.execute('SELECT dt, temperature, wind_speed, type FROM weather WHERE city_name = ? AND dt > ? ORDER BY DT ASC',
                                     (city, (now - delta).timestamp())).fetchall()
                    # print(data)

                    s_list = "\n\n".join([
                        "На момент {}:\n"
                        "Температура => <b>{} °C</b>\n"
                        "Скорость ветра => <b>{} м/с</b>\n"
                        "Тип погоды => <b>{}</b>".format(datetime.datetime.fromtimestamp(d[0]), d[1], d[2], d[3]) for d in data
                    ])
                    text = "Выдаю историю о погоде в городе {}:\n\n{}".format(city, s_list)
                    update.message.reply_text(text, reply_markup=self.reply_markup, parse_mode=telegram.ParseMode.HTML)

    def cmd_call_last_cmd(self, bot, update, job_queue):
        try:
            num = int(update.message.text)
        except ValueError:
            last_cmd = self.last_cmd.get(update.message.chat_id)
            if last_cmd is not None:
                last_cmd(bot, update, job_queue, args=(update.message.text,))
        else:
            last_cities = self.last_cities.get(update.message.chat_id, [])
            if num > 0 and num <= len(last_cities):
                job_queue.run_once(self.query_weather_callback, 0, context=(update.message.chat_id, '',
                                                                            last_cities[num - 1]['id']))
            else:
                update.message.reply_text("Некорректный ввод, попробуйте ещё раз", reply_markup=self.reply_markup,
                                          parse_mode=telegram.ParseMode.HTML)

    def cmd_unknown_cmd(self, bot, update):
        update.message.reply_text("Ой, я что-то вас не понял. Смотрите, что я умею:\n\n"
                                  "<b>/cweather</b> [город]\t-\tи я выведу текущую погоду в указанном городе\n"
                                  "<b>/history</b> [город]\t-\tи я покажу тебе информацию о погоде за прошлую неделю",
                                  reply_markup=self.reply_markup, parse_mode=telegram.ParseMode.HTML)


def main():
    exitter = SignalExitter()

    bot = WeatherBot('weather.db', '4ea24a004b00e53a11d9efe44554459d', '465538893:AAGxP-jn5RTj5t0b8X_-2Q9zo6e63imUxCU')
    bot.start()

    exitter.wait()

    bot.stop()


if __name__ == '__main__':
    main()
