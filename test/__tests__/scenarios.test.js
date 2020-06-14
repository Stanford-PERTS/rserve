import countBy from 'lodash/countBy';
import faker from 'faker';
import forEach from 'lodash/forEach';
import groupBy from 'lodash/groupBy';
import moment from 'moment';

import mocksTriton from '../triton/src/mocks';
import { initialize, cleanup, insertData, runR } from './helpers';
import { getSurveyConfig, createCycleResponses } from './mocks';
import {
  // addTeamsToCommunity,
  fakeCommunity,
  // fakeParticipation,
  createTeamScenario,
} from './dbFaker';

// rserve reports take longer than 30 seconds
jest.setTimeout(60000);

// Sets up the database and tables. This only needs to be done once because
// each test will create its own independent set of objects/rows, and RServe
// will only be asked to process those reporting units.
beforeAll(() => initialize());
afterAll(() => cleanup());

describe('Report Scenarios', () => {
  const surveyLabel = 'beleset19';
  const surveyConfig = getSurveyConfig(surveyLabel);
  const reportTemplate = 'beleset_cset_report';

  it('basic report project', async () => {
    const instaRender = false;

    // https://docs.google.com/spreadsheets/d/1sLYk04SSI1wduWVCDPc4ro-BIv2Fp2Ar7GTP4CS2_6I/edit#gid=2079636849
    const allLearningConditions = surveyConfig.learningConditions;
    const noOpenResponses = [];
    const numClassrooms = 2;
    const numCycles = 2;
    const numPerClassroom = 20;
    // no target groups

    // CREATE TEST DATA


    const label = faker.company.bs()
    const program = mocksTriton.createProgram({label, name: label });
    const entities = createTeamScenario(program, allLearningConditions, {
      numClassrooms,
      numCycles,
      numPerClassroom,
    });

    const {
      classrooms,
      cycles,
      team,
      teamCaptain,
      teamContacts,
      tritonParticipants, // 20 * 2 = 40
    } = entities;

    const disadv = { race_african_american: true };
    const adv = { race_default: false, race_european: true };
    const forcedAnswersCycle1 = [
      // 10 students per classroom marked a disadvantaged race
      // group (Black or African American checked)
      [5, { gender: 'male', ...disadv }],
      [5, { gender: 'female', ...disadv }],
      // 10 per classroom marked ONLY advantaged race groups
      // (European / European American)
      [5, { gender: 'male', ...adv }],
      [5, { gender: 'female', ...adv }],
    ];

    // One Response will be generated for each Participant for each Cycle, for
    // each classroom they're in.
    const { neptuneParticipants, responses } = createCycleResponses(
      surveyConfig,
      tritonParticipants,
      classrooms,
      cycles,
      allLearningConditions,
      noOpenResponses,
      [forcedAnswersCycle1],
    );

    // INSERT TEST DATA INTO DATABASE

    await insertData({
      users: [teamCaptain, ...teamContacts],
      teams: [team],
      classrooms,
      cycles,
      programs: [program],
      tritonParticipants,
      neptuneParticipants,
      responses,
    });

    // ASSERTIONS

    expect(team.uid).toContain('Team_');
    expect(classrooms).toHaveLength(numClassrooms);
    classrooms.forEach(c => expect(c.num_students).toBe(numPerClassroom));

    expect(cycles).toHaveLength(numCycles);
    expect(tritonParticipants).toHaveLength(numClassrooms * numPerClassroom);
    expect(neptuneParticipants).toHaveLength(numClassrooms * numPerClassroom);

    // Second cycle should be active.
    // https://momentjs.com/docs/#/query/is-between/
    const [, secondCycle] = cycles;
    const today = moment();
    const isSecondCycleActive = today.isBetween(
      secondCycle.start_date,
      secondCycle.end_date,
      'day', // granularity of day
      '[]', // start/end dates inclusive
    );
    expect(isSecondCycleActive).toBeTruthy();

    // Participant IDs in responses should match **Neptune** IDs.
    const neptuneIds = neptuneParticipants.map(p => p.uid);
    const saturnIds = responses.map(r => r.participant_id);
    expect(saturnIds.every(id => neptuneIds.includes(id)));

    // Responses marked `saw_demographics` should NOT answer demographics Qs.
    const responsesWithSawDemographics = responses.filter(
      r => r.meta.saw_demographics,
    );
    for (const response of responsesWithSawDemographics) {
      expect(response.answers).not.toHaveProperty('gender');
      expect(response.answers).not.toHaveProperty('race_amindian');
    }

    // Responses NOT marked `saw_demographics` SHOULD answer demographics Qs.
    const responsesWithoutSawDemographics = responses.filter(
      r => !r.meta.saw_demographics,
    );
    for (const response of responsesWithoutSawDemographics) {
      expect(response.answers).toHaveProperty('gender');
      expect(response.answers).toHaveProperty('race_amindian');
    }

    // Should have answered female.
    const responsesWithFemale = responses.filter(
      r => r.answers.gender === 'female',
    );
    expect(responsesWithFemale).toHaveLength(10 * numClassrooms);

    // Should have answered male.
    const responsesWithMale = responses.filter(
      r => r.answers.gender === 'male',
    );
    expect(responsesWithMale).toHaveLength(10 * numClassrooms);

    // Should have answered no open response questions.
    forEach(responses, response => {
      forEach(response.answers, (answer, questionName) => {
        expect(questionName).not.toContain('or_');
      });
    });

    // Verify total number of responses we have generated.
    expect(responses).toHaveLength(numClassrooms * numPerClassroom * numCycles);

    // All Participants participated in all Cycles.
    for (const participant of neptuneParticipants) {
      const participantResponses = responses
        // filter to this participants responses
        .filter(r => r.meta.participant_id === participant.uid)
        // sort by date for easier asserts below
        .sort((rA, rB) => rA.modifiedOn.seconds - rB.modifiedOn.seconds);

      // Two responses per participant.
      expect(participantResponses).toHaveLength(numCycles);

      // First participation was in cycle 1.
      expect(
        moment(participantResponses[0].modifiedOn.seconds, 'X').isBetween(
          cycles[0].start_date,
          cycles[0].end_date,
          'day',
          '[]',
        ),
      ).toBeTruthy();

      // Second participation was in cycle 2.
      expect(
        moment(participantResponses[1].modifiedOn.seconds, 'X').isBetween(
          cycles[1].start_date,
          cycles[1].end_date,
          'day',
          '[]',
        ),
      ).toBeTruthy();

      // Students are marked race_african_american
      const responsesWithAfricanAmerica = responses.filter(
        r => r.answers.race_african_american === true,
      );
      expect(responsesWithAfricanAmerica).toHaveLength(
        // Will only answer demographics questions in one cycle.
        10 * numClassrooms,
      );

      // Students are marked race_european
      const responsesWithEuropean = responses.filter(
        r => r.answers.race_african_american === true,
      );
      expect(responsesWithEuropean).toHaveLength(
        // Will only answer demographics questions in one cycle.
        10 * numClassrooms,
      );
    }

    const reportingUnitIds = [team.uid, ...classrooms.map(c => c.uid)];

    const result = await runR(
      'beleset.R',
      reportingUnitIds,
      instaRender && reportTemplate,
    );

    forEach(classrooms, classroom => {
      expect(result).toHaveProperty(classroom.uid);
    });

    expect(result).toHaveProperty(team.uid);
  });

  it('makes no report for active but empty cycle', async () => {
    const instaRender = false;

    // https://docs.google.com/spreadsheets/d/1sLYk04SSI1wduWVCDPc4ro-BIv2Fp2Ar7GTP4CS2_6I/edit#gid=2079636849
    const allLearningConditions = surveyConfig.learningConditions;
    const noOpenResponses = [];
    const noForcedAnswers = [];
    const numClassrooms = 2;
    const numCycles = 2;
    const numPerClassroom = 20;
    // no target groups

    // CREATE TEST DATA

    const label = faker.company.bs()
    const program = mocksTriton.createProgram({label, name: label });
    const entities = createTeamScenario(program, allLearningConditions, {
      numClassrooms,
      numCycles,
      numPerClassroom,
    });

    const {
      classrooms,
      cycles,
      team,
      teamCaptain,
      teamContacts,
      tritonParticipants, // 20 * 2 = 40
    } = entities;

    // Only generate data for the first of the two cycles.
    const { neptuneParticipants, responses } = createCycleResponses(
      surveyConfig,
      tritonParticipants,
      classrooms,
      [cycles[0]],
      allLearningConditions,
      noOpenResponses,
      noForcedAnswers,
    );

    // Make sure that cycle 2 is current: today is <= end date
    expect(moment().isSameOrBefore(cycles[1].end_date));
    // expect(cycles[1].end_date).toBe(moment().format('YYYY-MM-DD'));

    // Make sure that all responess are in cycle 1.
    expect(
      responses
        .map(r => moment(r.modifiedOn.seconds * 1000).format('YYYY-MM-DD'))
        .every(t => t <= cycles[0].end_date),
    ).toBe(true);

    // INSERT TEST DATA INTO DATABASE

    await insertData({
      users: [teamCaptain, ...teamContacts],
      teams: [team],
      classrooms,
      cycles,
      programs: [program],
      tritonParticipants,
      neptuneParticipants,
      responses,
    });

    const reportingUnitIds = [team.uid, ...classrooms.map(c => c.uid)];

    const result = await runR(
      'beleset.R',
      reportingUnitIds,
      instaRender && reportTemplate,
    );

    // There should be no reports, but a reason for each should be provided.
    expect(result).toHaveProperty(team.uid);
    expect(result[team.uid]).toEqual('not_recently_cycled');
    expect(result).toHaveProperty(classrooms[0].uid);
    expect(result[classrooms[0].uid]).toEqual('not_recently_cycled');
    expect(result).toHaveProperty(classrooms[1].uid);
    expect(result[classrooms[1].uid]).toEqual('not_recently_cycled');
  });

  it('runs default community scenario', async () => {
    const instaRender = false;

    const allLearningConditions = surveyConfig.learningConditions;
    const noOpenResponses = [];
    const numClassrooms = 2;
    const numCycles = 2;
    const numPerClassroom = 20;
    // no target group

    // CREATE TEST DATA

    const label = faker.company.bs()
    const program = mocksTriton.createProgram({label, name: label });
    const community = fakeCommunity({ program_id: program.uid });
    const entities = createTeamScenario(program, allLearningConditions, {
      community,
      numClassrooms,
      numCycles,
      numPerClassroom,
    });

    const {
      team,
      classrooms,
      cycles,
      tritonParticipants,
      teamCaptain,
      teamContacts,
    } = entities;

    const disadv = { race_african_american: true };
    const adv = { race_default: false, race_european: true };
    const forcedAnswersCycle1 = [
      // 10 students per classroom marked a disadvantaged race
      // group (Black or African American checked)
      [5, { gender: 'male', ...disadv }],
      [5, { gender: 'female', ...disadv }],
      // 10 per classroom marked ONLY advantaged race groups
      // (European / European American)
      [5, { gender: 'male', ...adv }],
      [5, { gender: 'female', ...adv }],
    ];

    // One Response will be generated for each Participant for each Cycle.
    const { neptuneParticipants, responses } = createCycleResponses(
      surveyConfig,
      tritonParticipants,
      classrooms,
      cycles,
      allLearningConditions,
      noOpenResponses,
      [forcedAnswersCycle1],
    );

    // INSERT TEST DATA INTO DATABASE

    await insertData({
      users: [teamCaptain, ...teamContacts],
      organizations: [community],
      programs: [program],
      teams: [team],
      classrooms,
      cycles,
      tritonParticipants,
      neptuneParticipants,
      responses,
    });

    // ASSERTIONS

    expect(community.uid).toContain('Organization_');
    expect(team.organization_ids).toContain(community.uid);

    const reportingUnitIds = [community.uid];
    const result = await runR(
      'beleset.R',
      reportingUnitIds,
      instaRender && reportTemplate,
    );

    const nextMonday = // actually "today or next monday"
      moment().day() === 1
        ? moment()
        : moment()
            .startOf('isoWeek')
            .add(1, 'week');
    const nextMondayStr = nextMonday.format('YYYY-MM-DD');

    const dataset = result[community.uid];
    expect(dataset).toMatchObject({
      filename: `${nextMondayStr}.html`,
      id: community.uid,
      items: expect.any(Array), // will assert this separately
      organization_id: community.uid,
      organization_name: community.name,
      report_date: nextMondayStr,
      teams: {
        [team.uid]: {
          name: team.name,
          short_uid: team.short_uid,
        },
      },
      use_open_responses: true,
    });

    const { items } = dataset;
    items.forEach(item => {
      expect(item).toMatchObject({
        // check values below
        delta_table: {
          columns: expect.any(Array),
          rows: expect.any(Array),
        },
        item_id: expect.stringMatching(/.+/),
        label: expect.any(String), // question text
        team_by_week_plots: expect.any(Array),
        learning_condition_label: expect.any(String), // check values below
      });
      item.delta_table.rows.forEach(row => {
        const rowKeys = Object.keys(row);
        expect(rowKeys).toEqual(
          expect.arrayContaining(item.delta_table.columns),
        );
        expect(row.team_id).toBe(team.uid);
        expect(row.Project).toBe(team.name);
        const nonPercentKeys = ['team_id', 'Project'];
        for (const key of rowKeys) {
          if (!nonPercentKeys.includes(key)) {
            // e.g. 0%, +100%, -55%, or an emdash character
            expect(row[key]).toMatch(/^([+-]?\d{1,3}%|—)$/); // allows mdash
          }
        }
      });
    });

    // Three questions per learning condition.
    const itemsPerLC = countBy(items, i => i.learning_condition_label);
    expect(itemsPerLC).toEqual({
      'feedback-for-growth': 3,
      'meaningful-work': 3,
      'teacher-caring': 3,
      'cultural-competence': 3,
      'student-voice': 3,
      'classroom-belonging': 3,
    });
  });

  it('omits "items" if only one week of responses', async () => {
    const instaRender = false;

    const allLearningConditions = surveyConfig.learningConditions;
    const noForcedAnswers = [];
    const noOpenResponses = [];
    const numClassrooms = 2;
    const numCycles = 1; // <---
    const numPerClassroom = 20;
    // no target group

    // CREATE TEST DATA

    const label = faker.company.bs()
    const program = mocksTriton.createProgram({label, name: label });
    const community = fakeCommunity({ program_id: program.uid });
    const entities = createTeamScenario(program, allLearningConditions, {
      community,
      numClassrooms,
      numCycles,
      numPerClassroom,
    });

    const {
      team,
      classrooms,
      cycles,
      tritonParticipants,
      teamCaptain,
      teamContacts,
    } = entities;

    // One Response will be generated for each Participant for each Cycle.
    const { neptuneParticipants, responses } = createCycleResponses(
      surveyConfig,
      tritonParticipants,
      classrooms,
      cycles,
      allLearningConditions,
      noOpenResponses,
      noForcedAnswers,
    );

    // INSERT TEST DATA INTO DATABASE

    await insertData({
      users: [teamCaptain, ...teamContacts],
      organizations: [community],
      programs: [program],
      teams: [team],
      classrooms,
      cycles,
      tritonParticipants,
      neptuneParticipants,
      responses,
    });

    // ASSERTIONS

    const reportingUnitIds = [community.uid];
    const result = await runR(
      'beleset.R',
      reportingUnitIds,
      instaRender && reportTemplate,
    );
    const dataset = result[community.uid];

    expect(dataset.items).toHaveLength(0);
  });

  it('all open responses on', async () => {
    const instaRender = false;

    // https://docs.google.com/spreadsheets/d/1sLYk04SSI1wduWVCDPc4ro-BIv2Fp2Ar7GTP4CS2_6I/edit#gid=2079636849
    const numClassrooms = 2;
    const allLearningConditions = surveyConfig.learningConditions;
    const numPerClassroom = 20;
    const numCycles = 2;
    const allOpenResponsesOn = allLearningConditions;
    // no target group

    // CREATE TEST DATA

    const label = faker.company.bs()
    const program = mocksTriton.createProgram({label, name: label });
    const { team, classrooms, cycles, tritonParticipants } = createTeamScenario(
      program,
      surveyConfig.learningConditions,
      {
        numClassrooms,
        numCycles,
        numPerClassroom,
      },
    );

    const disadv = { race_african_american: true };
    const adv = { race_default: false, race_european: true };
    const forcedAnswersCycle1 = [
      // 10 students per classroom marked a disadvantaged race
      // group (Black or African American checked)
      [5, { gender: 'male', ...disadv }],
      [5, { gender: 'female', ...disadv }],
      // 10 per classroom marked ONLY advantaged race groups
      // (European / European American)
      [5, { gender: 'male', ...adv }],
      [5, { gender: 'female', ...adv }],
    ];

    // One Response will be generated for each Participant for each Cycle.
    const { neptuneParticipants, responses } = createCycleResponses(
      surveyConfig,
      tritonParticipants,
      classrooms,
      cycles,
      allLearningConditions,
      allOpenResponsesOn,
      [forcedAnswersCycle1],
    );

    // INSERT TEST DATA INTO DATABASE

    await insertData({
      teams: [team],
      classrooms,
      cycles,
      programs: [program],
      tritonParticipants,
      neptuneParticipants,
      responses,
    });

    // ASSERTIONS

    forEach(responses, response => {
      // Should flag one open response question per learning condition to be
      // answered.
      expect(response.meta.showOpenResponses).toHaveLength(
        allOpenResponsesOn.length,
      );

      // Should have answered all flagged open response questions.
      forEach(response.meta.showOpenResponses, lcQuestionName => {
        expect(response.answers).toHaveProperty(lcQuestionName);
      });
    });

    /////////////// run?
  });

  // Target group feature ON
  it('displays target group info', async () => {
    const instaRender = false;

    const allLearningConditions = surveyConfig.learningConditions;
    const noOpenResponses = [];
    const noForcedAnswers = [];
    const targetGroupName = 'Foo Group';


    const label = faker.company.bs()
    const program = mocksTriton.createProgram({label, name: label });
    const community = fakeCommunity({ program_id: program.uid });
    const entities = createTeamScenario(program, allLearningConditions, {
      community,
      // Fake two classrooms to make sure the team report is rendered; the
      // default behavior of RServe is to skip the team report when it is
      // redundant with a solo classroom report.
      numClassrooms: 2,
      numCycles: 2,
      numPerClassroom: 20,
      targetGroupName,
      ratioInTargetGroup: 0.5, // i.e. 10
    });
    const {
      classrooms,
      cycles,
      team,
      teamCaptain,
      teamContacts,
      tritonParticipants, // 20 * 2 = 40
    } = entities;

    // One Response will be generated for each Participant for each Cycle.
    const { neptuneParticipants, responses } = createCycleResponses(
      surveyConfig,
      tritonParticipants,
      classrooms,
      cycles,
      allLearningConditions,
      noOpenResponses,
      noForcedAnswers,
    );

    // //// Make sure the scenario is constructed correctly.

    expect(team.target_group_name).toBe(targetGroupName);

    const byClass = groupBy(tritonParticipants, p => p.classroom_ids[0]);
    for (const cid of Object.keys(byClass)) {
      expect(byClass[cid].length).toBe(20);
      expect(byClass[cid].filter(p => p.in_target_group).length).toBe(10);
    }

    // //// Write to DB

    await insertData({
      users: [teamCaptain, ...teamContacts],
      organizations: [community],
      programs: [program],
      teams: [team],
      classrooms,
      cycles,
      tritonParticipants,
      neptuneParticipants,
      responses,
    });

    // //// Run RServe

    // Only run one of the two classrooms just to save time.
    const [classroom] = classrooms;
    const reportingUnitIds = [team.uid, classroom.uid, community.uid];
    const result = await runR(
      'beleset.R',
      reportingUnitIds,
      instaRender && reportTemplate,
    );

    // //// Check datasets

    const teamDataset = result[team.uid];
    const classDataset = result[classroom.uid];
    const communityDataset = result[community.uid];

    const checkBarChart = lc => {
      // There should be an even number of target group and non-target group
      // rows in the bar chart.
      const tgRows = lc.bar_chart_df.filter(
        r => r.subset_type === 'target_group_cat',
      );
      expect(tgRows.length).toBeGreaterThan(0);
      expect(tgRows.length % 2).toBe(0); // even
      expect(tgRows.filter(r => r.subset_value === 'Target Grp.').length).toBe(
        tgRows.length / 2,
      );
      expect(
        tgRows.filter(r => r.subset_value === 'Not Target Grp.').length,
      ).toBe(tgRows.length / 2);

      // Target group bars should not be masked.
      expect(tgRows.every(row => row.mask_type === false)).toBe(true);
    };

    teamDataset.learning_conditions.forEach(checkBarChart);
    classDataset.learning_conditions.forEach(checkBarChart);

    communityDataset.items.forEach(item => {
      expect(item.delta_table.columns).toContain('Target Grp.');
      item.delta_table.rows.forEach(row => {
        expect(Object.keys(row)).toContain('Target Grp.');
      });
    });

    // Classroom report:
    // - ggplot shows target group data (7 levels of `subset_value`)
    // Team report:
    // - ggplot shows target group data (7 levels of `subset_value`)
    // Community report:
    // - delta tables have target group column
  });
});
