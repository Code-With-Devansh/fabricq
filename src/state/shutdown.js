let shuttingDown = false;

export const setShuttingDown = () => {
  shuttingDown = true;
};

export const isShuttingDown = () => shuttingDown;