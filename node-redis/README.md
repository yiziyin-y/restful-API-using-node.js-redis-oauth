# API with NodeJS and REDIS
## NodeJS Installation
You can install NodeJS by downloading the latest binaries for your desired operating system from their [official download page](https://nodejs.org/en/download/current/).

## Redis Installation

### Mac
![redis](https://res.cloudinary.com/ichtrojan/image/upload/v1535585936/Screenshot_2018-08-30_at_12.38.41_AM_rvrkp5.png)

you can install redis uning [homebrew](http://brew.sh).

run `brew install redis` in terminal to install redis on your mac using homebrew, assuming you have homebrew installed.

### Linux

You can learn how to install redis on Linux [here](https://community.pivotal.io/s/article/How-to-install-and-use-Redis-on-Linux)

### Windows

You can learn how to install redis on Windows [here](https://redislabs.com/ebook/appendix-a/a-3-installing-on-windows/a-3-2-installing-redis-on-window/)

## Setting up

![npm start](https://res.cloudinary.com/ichtrojan/image/upload/v1535585500/Screenshot_2018-08-30_at_12.31.20_AM_vluh0e.png)

* clone the repo
* change directory
* run `npm install`
* run `npm start`
* visit http://localhost:4040


## Using Postman
Make sure post is set to `x-www-form-urlencoded`
* add plan (POST): `/users/`
* delete plan (DELETE): `/users/{id}`
* get a plan(GET): `/users/{id}`
* update a plan (PUT): `/users/{id}`
* patch a plan (PATCH): `/users/{id}`
* get all plan (GET): `/users`
