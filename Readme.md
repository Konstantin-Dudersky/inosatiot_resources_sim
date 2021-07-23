# Установка
1. Скачать проект с github
   
        $ mkdir ~/inosatiot && cd ~/inosatiot && sudo apt install git
        $ git clone https://github.com/Konstantin-Dudersky/inosatiot_resources_sim.git
        $ cd inosatiot_resources_sim

2. Создать файл с настройками inosatiot_cfg.json. Шаблон находится в setup/inosatiot_cfg.json_template.
       
        $ cp setup/config_example.yaml ../config_inosatiot_resources_sim.yaml

   Прописать в файле настройки.


3. Установка библиотек python в файле setup/setup.sh
   
        $ chmod +x setup/setup.sh && setup/setup.sh

   Что делает скрипт:
   - Сохраняет в переменную среды путь к файлу inosatiot_cfg.json
   - Обновляет пакеты в системе
   - Устанавливает пакеты python
   - Создает виртуальное окружение venv, скачивает необходимые пакеты
   - Создает сервис systemd, устанавливает автозапуск
   - Открывает сетевой доступ к папке с отчетами
    
    
4. После установки можно запустить на выполнение через systemd
   
        $ sudo systemctl start inosatiot_report.service  // запустить
        $ sudo systemctl stop inosatiot_report.service  // остановить
        $ sudo systemctl restart inosatiot_report.service  // перезапустить
        $ sudo systemctl status inosatiot_report.service  // просмотреть статус

# Обновить проект
- Синхронизировать проект с github (локальные изменения теряются)
   
        $ git fetch origin && git reset --hard origin/master && git clean -f -d
        $ chmod +x setup/setup.sh && setup/setup.sh

