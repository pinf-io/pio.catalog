#!/bin/bash

initScript='
description "{{=service.env.PIO_SERVICE_ID_SAFE}}"

start on local-filesystems
stop on shutdown

script
    echo $$ > {{=service.env.PIO_SERVICE_RUN_BASE_PATH}}.pid
    export PATH={{=service.env.PATH}}
    export PORT={{=service.env.PORT}}
    exec node {{=service.env.PIO_SERVICE_PATH}}/live/install/server.js >> {{=service.env.PIO_SERVICE_LOG_BASE_PATH}}.log 2>&1
end script

pre-start script
    echo "\\n\\n['`date -u +%Y-%m-%dT%T.%3NZ`'] (/etc/init/app-{{=service.env.PIO_SERVICE_ID_SAFE}}.conf) ########## STARTING ##########\\n" >> {{=service.env.PIO_SERVICE_LOG_BASE_PATH}}.log
end script

pre-stop script
    rm -f {{=service.env.PIO_SERVICE_RUN_BASE_PATH}}.pid
    echo "\\n['`date -u +%Y-%m-%dT%T.%3NZ`'] (/etc/init/app-{{=service.env.PIO_SERVICE_ID_SAFE}}.conf) ^^^^^^^^^^ STOPPING ^^^^^^^^^^\\n\\n" >> {{=service.env.PIO_SERVICE_LOG_BASE_PATH}}.log
end script
'

sh $PIO_SCRIPTS_PATH/stop.sh

echo "$initScript" | sudo tee /etc/init/app-$PIO_SERVICE_ID_SAFE.conf

sh $PIO_SCRIPTS_PATH/start.sh
