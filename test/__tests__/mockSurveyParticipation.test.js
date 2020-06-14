import * as mocks from './mocks';
import mocksTriton from '../triton/src/mocks';
import sampleSize from 'lodash/sampleSize';
import { cleanup, initialize } from './helpers';
import { insertResponse } from './dbSaturn';

describe('survey participation', () => {
  beforeEach(() => initialize());
  afterEach(() => cleanup());

  it('should mock a survey response', async () => {
    const surveyConfig = mocks.getSurveyConfig('beleset19');
    const learningConditions = [
      'classroom-belonging',
      'cultural-competence',
      'feedback-for-growth',
      'meaningful-work',
      'student-voice',
      'teacher-caring',
    ];
    const openResponses = sampleSize(learningConditions, 1);

    const classroom = mocksTriton.createClassroom();
    // const cycle = mocksTriton.createCycle({ team_id: classroom.team_id })

    const triParticipant = mocksTriton.createParticipant({
      team_id: classroom.team_id,
      classroom_ids: [classroom.uid],
    });
    const nepParticipant = mocks.convertParticipant(triParticipant);

    const overrideProps = {
      createdOn: '2020-01-01 11:30:00',
      modifiedOn: '2020-01-01 12:30:00',
      meta: {
        code: classroom.code,
        participant_id: nepParticipant.uid,
        saw_demographics: true,
        learning_conditions: learningConditions,
        open_response_lcs: openResponses,
      },
    };

    const responseForcedAnswers = {
      c_belonging_classmates: 'forced answer value',
    };

    const response = mocks.createResponse(
      surveyConfig,
      overrideProps,
      responseForcedAnswers,
    );

    expect(surveyConfig.label).toBe('beleset19');
    expect(classroom).toHaveProperty('uid');
    expect(triParticipant).toHaveProperty('uid');
    expect(nepParticipant).toHaveProperty('uid');
    expect(response).toHaveProperty('firestore_id');

    // Can force a desired answer.
    expect(response.answers.c_belonging_classmates).toBe('forced answer value');

    await insertResponse(response, response.firestore_id);
  });
});
