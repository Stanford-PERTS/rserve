// https://jestjs.io/docs/en/getting-started
// https://basarat.gitbook.io/typescript/intro-1/jest
// https://stackoverflow.com/questions/43281741/how-to-use-paths-in-tsconfig-json

import sample from 'lodash/sample';

import tritonMocks from 'triton/src/mocks';
import { cleanup, initialize } from './helpers';
import { createResponse, getSurveyConfig } from './mocks';
import { insertResponse } from './dbSaturn';
import { insertTriton } from './dbTriton';


describe('rserve reports', () => {
  beforeEach(() => initialize());
  afterEach(() => cleanup());

  describe('example tests', () => {
    it('should show toContain', () => {
      const options = [1, 2, 3, 4, 5];
      const randomSample = sample(options);

      expect(options).toContain(randomSample);
    });

    it('should do another thing', () => {
      const team = tritonMocks.createTeam({ name: 'Team Viper' });
      expect(team.name).toBe('Team Viper');
    });
  });

  describe('saturn examples', () => {
    it('should insert a response', async () => {
      const responseDocumentId = 'f83728hfjds@#$RK';
      await insertResponse(
        {
          createdOn: { seconds: 0, nanoseconds: 0 },
          modifiedOn: { seconds: 0, nanoseconds: 0 },
          surveyLabel: 'cset19',
          meta: {
            code: 'angel fish',
            participant_id: 'Participant_123456789',
            survey_id: 'Survey_123456789',
          },
          answers: {},
          progress: 1,
          page: 'introduction',
        },
        responseDocumentId,
      );
    });

    it('should mock a response', () => {
      const surveyLabel = 'beleset19';
      const beleset19 = getSurveyConfig(surveyLabel);
      const response = createResponse(beleset19);

      expect(response).toHaveProperty('meta');
      expect(response).toHaveProperty('meta.code');
      expect(response).toHaveProperty('meta.participant_id');
      expect(response).toHaveProperty('meta.survey_id');
      expect(response.surveyLabel).toBe(surveyLabel);
    });
  });

  describe('triton examples', () => {
    it('should insert a team', async () => {
      const team = tritonMocks.createTeam();
      await insertTriton('team', team);
    });

    it('should insert a classroom', async () => {
      const classroom = tritonMocks.createClassroom();
      await insertTriton('classroom', classroom);
    });
  });
});
