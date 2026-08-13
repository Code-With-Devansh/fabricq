FROM node:22-alpine

WORKDIR /app
# argon2 needs build tools to compile/install its native binding on alpine
RUN apk add --no-cache python3 make g++

COPY package*.json ./

RUN npm install

COPY . .

EXPOSE 3000

CMD ["npm", "start"]