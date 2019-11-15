## Running

```bash
$ docker run -d -p 9411:9411 \
  -e STORAGE_TYPE=berkeley \
  -e BERKELEY_DB_PATH=/etc/berkeley \
  jkschneider/zipkin-berkeley
```