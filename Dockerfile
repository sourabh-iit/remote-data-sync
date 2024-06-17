FROM node:20-alpine
RUN mkdir -p /home/app
WORKDIR /home/app
COPY package*.json ./
RUN npm install
COPY . .
RUN touch .env
CMD [ "npm", "start" ]