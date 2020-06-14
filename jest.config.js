// For a detailed explanation regarding each configuration property, visit:
// https://jestjs.io/docs/en/configuration.html

const path = require('path');

module.exports = {
  // Global variables that need to be available in all test environments
  globals: {
    window: {},
  },

  // An array of directory names to be searched recursively up from the
  // requiring module's location
  moduleDirectories: [
    'node_modules',
    path.join(__dirname, 'test'),
    path.join(__dirname, 'test/__tests__'),
    path.join(__dirname, 'test/triton/src'),
    path.join(__dirname, 'test/neptune/src'),
    path.join(__dirname, 'test/saturn/hosting/src'),
  ],

  // https://jestjs.io/docs/en/configuration#setupfilesafterenv-array
  setupFilesAfterEnv: ['./jest.setup.js'],

  // The test environment that will be used for testing
  testEnvironment: 'node',

  // The glob patterns Jest uses to detect test files
  testMatch: ['**/__tests__/**/?(*.)+(spec|test).+(ts|tsx|js)'],

  // A map from regular expressions to paths to transformers
  transform: {
    '^.+\\.(ts|tsx|.d.ts|js)$': 'ts-jest',
  },
};
