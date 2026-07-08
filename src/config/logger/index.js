
import productionLogger from './productionLogger.js';
import devLogger from './devLogger.js';
import config from '../index.js';

const logger = config.app.env === 'production'
  ? productionLogger
  : devLogger;

export default logger;