import express from 'express';
import morgan from 'morgan';

const app = express();

app.use(express.json());
app.use(morgan('combined'));

app.get('/health', (_req, res) => {
  res.status(200).json({ status: 'ok', service: 'notification-service' });
});

export default app;
