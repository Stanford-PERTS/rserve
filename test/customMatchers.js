// For example see jest.setup.js
// For docs see https://jestjs.io/docs/en/expect#custom-matchers-api

export const toBeNonEmptyString = s => {
  const typ = typeof s;
  const isString = typ === 'string';
  const length = isString ? s.length : undefined;

  return {
    pass: isString && length > 0 === true,
    message: () =>
      `Not a non-empty string. Received: ${s}, type: ${typ}, length: ${length}`,
  };
};

// Example: expect(possiblyNull).toBeNonEmptyStringOr(null)
export const toBeNonEmptyStringOr = (x, orValue) =>
  x === orValue ? { pass: true } : toBeNonEmptyString(x);

export const toBeNonNegativeNumber = n => {
  const typ = typeof n;
  const nonNegative = n >= 0;

  return {
    pass: typ === 'number' && nonNegative === true,
    message: () =>
      `Not a non-negative number. Received: ${n}, ` +
      `type: ${typ}, >= 0: ${nonNegative}`,
  };
};
