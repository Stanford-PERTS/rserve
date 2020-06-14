import { createClassroomCode } from './mocks';
import mocksTriton from '../triton/src/mocks';
import * as faker from 'faker';
import capitalize from 'capitalize';
import moment from 'moment/moment';
import sampleSize from 'lodash/sampleSize';

export const addTeamsToCommunity = (community, teams) =>
  teams.map(t => ({
    ...t,
    organization_ids: t.organization_ids.concat(community.uid),
  }));

export const createUid = (type = '') => {
  const uid = faker.random.alphaNumeric(12);
  return `${capitalize(type)}_${uid}`;
};

export const fakeCommunity = options => ({
  uid: createUid('Organization'),
  name: faker.random.words(2),
  code: faker.random.alphaNumeric(12),
  ...options,
});

export const fakeParticipantsForTeam = (
  team,
  classrooms,
  { numPerClassroom, ratioInTargetGroup, participationRate },
) => {
  let tritonParticipants = [];

  for (const c of classrooms) {
    const ppts = Array.from({ length: numPerClassroom }).map(_ =>
      mocksTriton.createParticipant({
        team_id: c.team_id,
        classroom_ids: [c.uid],
      }),
    );
    const numInTargetGroup = Math.floor(numPerClassroom * ratioInTargetGroup);
    sampleSize(ppts, numInTargetGroup).forEach(p => {
      p.in_target_group = true;
    });
    tritonParticipants = tritonParticipants.concat(ppts);
  }

  const numActive = Math.floor(tritonParticipants.length * participationRate);
  const activeParticipants = sampleSize(tritonParticipants, numActive);
  const neptuneParticipants = activeParticipants.map(p => ({
    uid: createUid('Participant'),
    organization_id: p.team_id,
    name: p.stripped_student_id,
  }));

  return { neptuneParticipants, tritonParticipants };
};

export const fakeTeamWithRosters = (
  program,
  {
    community,
    cycleDates = [],
    numClassrooms,
    numCycles,
    numPerClassroom,
    ratioInTargetGroup,
    setCycleDates,
    targetGroupName,
    participationRate,
  },
) => {
  const captain = { uid: 'User_cap' };
  // @todo: insert into `triton.team`
  const teamId = createUid('Team');
  const team = {
    uid: teamId,
    short_uid: teamId.split('_')[1],
    name: 'Team Foo',
    target_group_name: targetGroupName,
    program_id: program.uid,
  };
  team.organization_ids = community ? [community.uid] : [];

  // @todo: insert into `triton.cycle`
  const cycles = [];
  for (let ordinal = 1; ordinal <= numCycles; ordinal += 1) {
    // Example for 2 cycles:
    // cycle 1:
    //   start -12 days
    //   end -6 days
    // cycle 2:
    //   start -5 days
    //   end +1 (tomorrow)
    const weeksAgo = numCycles - ordinal;
    const overrideDates = cycleDates[ordinal - 1];

    // Example for 2 cycles:
    // cycle 1:
    //   start -13 days
    //   end -7 days
    // cycle 2:
    //   start -6 days
    //   end +1 (tomorrow)
    const startDate = (overrideDates
      ? overrideDates[0]
      : moment().subtract((weeksAgo + 1) * 7 - 2, 'day')
    ).format('YYYY-MM-DD');
    const endDate = (overrideDates
      ? overrideDates[1]
      : moment().subtract(weeksAgo * 7 - 1, 'day')
    ).format('YYYY-MM-DD');

    cycles.push(
      mocksTriton.createCycle({
        ordinal,
        start_date: setCycleDates ? startDate : null,
        end_date: setCycleDates ? endDate : null,
        team_id: team.uid,
      }),
    );
  }

  const classrooms = Array.from({ length: numClassrooms }).map(_ =>
    mocksTriton.createClassroom({
      contact_id: captain.uid,
      team_id: team.uid,
      num_students: numPerClassroom,
    }),
  ); //

  const { neptuneParticipants, tritonParticipants } = fakeParticipantsForTeam(
    team,
    classrooms,
    {
      numPerClassroom,
      ratioInTargetGroup,
      participationRate,
    },
  );

  return { team, classrooms, cycles, neptuneParticipants, tritonParticipants };
};

export const createTeamScenario = (
  program,
  learningConditions,
  {
    community,
    cycleDates,
    numClassrooms = 2,
    numCycles = 2,
    numPerClassroom = 20,
    participationRate = 1,
    ratioInTargetGroup = 0,
    setCycleDates = true,
    targetGroupName = null,
  } = {},
) => {
  if (community && community.program_id != program.uid) {
    throw new Error("community.program_id doesn't match program");
  }

  const {
    classrooms,
    cycles,
    neptuneParticipants,
    team,
    tritonParticipants,
  } = fakeTeamWithRosters(program, {
    community,
    cycleDates,
    numClassrooms,
    numCycles,
    numPerClassroom,
    participationRate,
    ratioInTargetGroup,
    setCycleDates,
    targetGroupName,
  });

  // Create a captain for team
  const teamCaptain = mocksTriton.createUser();
  mocksTriton.setUserCaptain(teamCaptain, team);

  // Create a teacher for each classroom
  const teamContacts = [];

  for (const classroom of classrooms) {
    const teacher = mocksTriton.createUser();
    mocksTriton.join(teacher, team);
    classroom.contact_id = teacher.uid;
    teamContacts.push(teacher);
  }

  return {
    classrooms,
    cycles,
    neptuneParticipants,
    team,
    teamCaptain,
    teamContacts,
    tritonParticipants,
  };
};
