FROM node:slim
WORKDIR /app
COPY . /app/

# Install PostgreSQL development libraries
RUN npm install
CMD ["npm", "start"]