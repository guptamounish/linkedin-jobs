FROM apify/actor-node:18
COPY package*.json ./
RUN npm install --include=dev --audit=false
COPY . ./
CMD npm start
