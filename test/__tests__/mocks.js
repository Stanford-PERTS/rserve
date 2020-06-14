import moment from 'moment';

import createClassroomCode from '../saturn/hosting/src/mocks/createClassroomCode';
import createResponse from '../saturn/hosting/src/mocks/createResponse';
import createUid from '../saturn/hosting/src/mocks/createUid';
import getSurveyConfig from '../saturn/hosting/src/mocks/getSurveyConfig';
import shortUid from '../saturn/hosting/src/mocks/shortUid';
import { availableRaces } from '../saturn/hosting/src/surveys/config';

export {
  createClassroomCode,
  createResponse,
  createUid,
  getSurveyConfig,
  shortUid,
};

// From a description of the kind of answers fake participants from a classroom
// should give, which are of the form:
//
//   [ [ numParticipants, profile ], ... ]
//
// output answer profiles one at a time for individuals. When running out of
// specified profiles, continuing to call next() will just give undefined,
// which is fine, especially when the number of participants in the class
// happens to be greater than the number of specified profiles.
function* profileGenerator(answerProfiles) {
  for (const [numParticipants, profile] of answerProfiles) {
    if (profile.race_default !== undefined) {
      // Expand this magic property into all the available race questions.
      for (const race of availableRaces) {
        profile[race] = profile[race] || profile.race_default;
      }
      delete profile.race_default;
    }
    for (let x = 0; x < numParticipants; x += 1) {
      yield profile;
    }
  }
}

export const createParticipant = ({ organization_id, name }) => ({
  uid: createUid('Participant'),
  organization_id: organization_id || createUid('Organization'),
  name: name || createUid('Student'),
});

export const convertParticipant = tritonParticipant =>
  createParticipant({
    organization_id: tritonParticipant.team_id,
    name: tritonParticipant.stripped_student_id,
  });

export const createCycleResponses = (
  surveyConfig,
  tritonParticipants,
  classrooms,
  cycles,
  learningConditionsToAnswer = [],
  learningConditionsOpenResponsesToAnswer = [],
  forcedCycleAnswers = [],
) => {
  const responses = [];
  const nepPptByName = {};

  for (const cycle of cycles) {
    // If specific kinds of answers were requested for this cycle.
    const answerConfig = forcedCycleAnswers[cycle.ordinal - 1] || [];

    for (const c of classrooms) {
      // Start a new generator; the counts are defined per-classroom.
      const answerGen = profileGenerator(answerConfig);

      for (const triParticipant of tritonParticipants) {
        // Should this participant take the survey in this class?
        if (!triParticipant.classroom_ids.includes(c.uid)) {
          continue;
        }

        // Find or create a matching neptune participant.
        let nepParticipant = nepPptByName[triParticipant.stripped_student_id];
        if (!nepParticipant) {
          nepParticipant = convertParticipant(triParticipant);
          nepPptByName[nepParticipant.name] = nepParticipant;
        }

        // What time should we say the responses were recorded?
        // * We don't want to record responses in the future, since that never
        //   happens in real life, i.e. is undefined behavior.
        // * Don't choose today, because when that's the same as the report
        //   date, RServe will ignore it
        //   ('2020-03-02' >= '2020-03-02 00:00:00' is FALSE)
        // So we take the cycle end date or yesterday, whichever is first.
        const yesterday = moment()
          .subtract(1, 'day')
          .format('YYYY-MM-DD');
        const responseDate =
          cycle.end_date < yesterday ? cycle.end_date : yesterday;

        const response = createResponse(
          surveyConfig,
          {
            createdOn: `${responseDate} 12:00:00`,
            modifiedOn: `${responseDate} 12:00:00`,
            meta: {
              code: c.code,
              participant_id: nepParticipant.uid,
              // Typically, after the first cycle, they've seen and answered
              // the demographics questions.
              saw_demographics: cycle.ordinal > 1 ? 'true' : '',
              // Survey in Neptune maps 1-to-1 with Classroom, so subbing in
              // class uid is good enough.
              survey_id: `${c.uid}:cycle-${cycle.ordinal}`,
              learning_conditions: learningConditionsToAnswer,
              open_response_lcs: learningConditionsOpenResponsesToAnswer,
            },
          },
          answerGen.next().value || {},
        );

        responses.push(response);
      }
    }
  }

  return { neptuneParticipants: Object.values(nepPptByName), responses };
};
