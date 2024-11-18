# WABD
WABD stands for watermark integrated ABD.

## How to build
```
cd [Your Project Root]
make
```

## How to run
configure "address" field in [Your Project Root]/config.json.
```
cd [Your Project Root]

./bin/server -id 0  &
./bin/server -id 1  &
./bin/server -id 2  &

./bin/client -id 1
```