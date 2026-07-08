// Wraps an async controller so any rejected promise / thrown error
// is forwarded to Express's error-handling middleware via next(err).
export const asyncHandler = (fn) => (req, res, next) => {
  Promise.resolve(fn(req, res, next)).catch(next);
};