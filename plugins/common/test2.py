import telepot
import os
import logging
from module import get_dir_list

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

TELEGRAM_TOKEN = "7474280784:AAGlghijDfdqfww67ZGHFzfgt1kTEDxX0-w"

def handler(msg):
    content_type, chat_type, chat_id, msg_date, msg_id = telepot.glance(msg, long=True)

    if content_type == "text":

        str_message = msg["text"]

        print(str_message)
        if str_message[0:1] == "/":
            args = str_message.split(" ")

            print(args)
            command = args[0]
            del args[0]

            if command == "/dir":
                file_path = " ".join(args)
                if file_path.strip() == "":
                    bot.sendMessage(chat_id, "/dir [대상폴더] 로 입력해주세요.")
                else:
                    file_list = get_dir_list(file_path)
                    bot.sendMessage(chat_id, file_list)
            elif command[0:4] == "/get":
                file_path = " ".join(args)

                if os.path.exists(file_path):
                    try:
                        if command == "/getfile":
                            bot.sendDocument(chat_id, open(file_path, "rb"))
                        elif command == "/getimage":
                            bot.sendPhoto(chat_id, open(file_path, "rb"))
                        elif command == "/getaudio":
                            bot.sendAudio(chat_id, open(file_path, "rb"))
                        elif command == "/getvideo":
                            bot.sendVideo(chat_id, open(file_path, "rb"))
                    except Exception as e:
                        bot.sendMessage(chat_id, "파일 전송 실패 {}".format(e))
                else:
                    bot.sendMessage(chat_id, "파일이 존재하지 않습니다.")



bot = telepot.Bot(TELEGRAM_TOKEN)

bot.message_loop(handler, run_forever=True)
