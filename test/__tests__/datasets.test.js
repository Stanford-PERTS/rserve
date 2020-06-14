// https://jestjs.io/docs/en/getting-started
// https://basarat.gitbook.io/typescript/intro-1/jest
// https://stackoverflow.com/questions/43281741/how-to-use-paths-in-tsconfig-json

import groupBy from 'lodash/groupBy';
import moment from 'moment/moment';
import sampleSize from 'lodash/sampleSize';
import { initialize, cleanup } from './helpers';

import {
  fakeCommunity,
  fakeParticipation,
  createTeamScenario,
} from './dbFaker';
import {
  classReportTemplateRequirements,
  fakeClassDataset,
} from './templateRequirements';

jest.setTimeout(30000);

// Sets up the database and tables. This only needs to be done once because
// each test will create its own independent set of objects/rows, and RServe
// will only be asked to process those reporting units.
beforeAll(() => initialize());
afterAll(() => cleanup());

// These tests use an out-of-date mocking api, but are useful to document all
// the things we have the ambition to test.
describe.skip('BELE-SET reports', () => {
  const surveyLabel = 'beleset19';
  const learningConditions = [
    'student-voice',
    'classroom-belonging',
    'teacher-caring',
    'feedback-for-growth',
    'meaningful-work',
    'cultural-competence',
  ];

  // Basic project report
  it('runs default scenario', () => {
    const { team, classrooms } = createTeamScenario(
      surveyLabel,
      learningConditions,
    );

    expect(team.uid).toBeNonEmptyString();
    expect(classrooms.length).toBe(2);
    for (const c of classrooms) {
      expect(c.uid).toBeNonEmptyString();
    }

    // ???
  });

  // Basic community report
  it('runs default community scenario', () => {
    const community = fakeCommunity();
    const { team: team1 } = createTeamScenario(
      surveyLabel,
      learningConditions,
      { community },
    );
    const { team: team2 } = createTeamScenario(
      surveyLabel,
      learningConditions,
      { community },
    );

    // it associates teams with community
    expect(team1.organization_ids).toContain(community.uid);
    expect(team2.organization_ids).toContain(community.uid);

    // ???
  });

  // Solo-roster mode
  it('shows only one paneling variable in solo roster mode', () => {
    const { classrooms } = createTeamScenario(surveyLabel, learningConditions, {
      numClassrooms: 1,
    });

    expect(classrooms.length).toBe(1);
    // Classroom report:
    // - All data.frame inputs to ggplot calls should have only one unique value
    //   in the horizontal paneling variable
    // - All participation_table_df objects should have exactly two columns
  });

  // Open responses on
  it('shows open responses for all lcs', () => {
    const entities = createTeamScenario(surveyLabel, learningConditions, {
      openResponseLcs: learningConditions, // the full list, not just one
    });

    // Classroom report:
    // - All learning conditions show open responses for each LC
    // Team report:
    // - no open responses are displayed
  });

  // Not all LCs selected
  it('does not show omitted lc(s)', () => {
    const fewerLcs = sampleSize(
      learningConditions,
      learningConditions.length - 1,
    );
    const entities = createTeamScenario(surveyLabel, fewerLcs);

    // Classroom report:
    // - Does not show omitted lc
    // Team report:
    // - Does not show omitted lc
  });

  // No cycle dates set
  it('displays just one cycle if no dates set', () => {
    const community = fakeCommunity();
    const entities = createTeamScenario(surveyLabel, learningConditions, {
      community,
      setCycleDates: false,
    });

    // Classroom report:
    // - Cross sectional graph, but no longitudinal graph
    // Team report:
    // - Cross sectional graph, but no longitudinal graph
    // Community report:
    // - No delta tables
    // - single point on run chart
  });

  // Participation BEFORE first cycle
  it('ignores participation before first cycle', () => {
    // Default behavior is to fake participation for each cycle.
    const entities = createTeamScenario(surveyLabel, learningConditions);
    const { cycles, tritonParticipants } = entities;

    // Fake additional participation from _before_ the first cycle.
    const { neptuneParticipants } = fakeParticipation(
      surveyLabel,
      learningConditions,
      moment(cycles[0].start_date).subtract(1, 'week'),
      entities,
    );

    // check returns cycles, ppts
    expect(cycles).toBeInstanceOf(Array);
    expect(cycles.length).toBeGreaterThan(0);
    expect(tritonParticipants).toBeInstanceOf(Array);
    expect(tritonParticipants.length).toBeGreaterThan(0);
    expect(neptuneParticipants).toBeInstanceOf(Array);
    expect(neptuneParticipants.length).toBeGreaterThan(0);

    // Classroom report:
    // - ignores participation before first cycle
    // Team report:
    // - ignores participation before first cycle
  });

  // No participation in previous week
  it('does not run with no participation in previous week', () => {
    const community = fakeCommunity();
    const entities = createTeamScenario(surveyLabel, learningConditions, {
      community,
      includeParticipation: false,
    });
    const { cycles } = entities;

    // Fake participation for the first but not the second cycle.
    fakeParticipation(
      surveyLabel,
      learningConditions,
      moment(cycles[0].start_date), // two weeks ago
      entities,
    );

    // Classroom report: none
    // Team report: none
    // Community report: none
  });

  // Single cycle of data only
  it('does not show longitudinal graphs with single cycle of data', () => {
    const { cycles } = createTeamScenario(surveyLabel, learningConditions, {
      numCycles: 1,
    });

    // has only 1 cycle
    expect(cycles).toBeInstanceOf(Array);
    expect(cycles.length).toBe(1);
    // Classroom report:
    // - no longitudinal graph
    // Team report:
    // - no longitudinal graph
    // Community report:
    // - no delta table
  });

  // Participation BETWEEN cycles
  it('adds participation between cycles to previous cycle', () => {
    const cycleDates = [
      [moment().subtract(3 * 7 - 1, 'day'), moment().subtract(2 * 7, 'day')],
      [moment().subtract(1 * 7 - 1, 'day'), moment()],
    ];
    const entities = createTeamScenario(surveyLabel, learningConditions, {
      cycleDates,
      numCycles: 2,
    });

    // Fake additional participation between cycles
    fakeParticipation(
      surveyLabel,
      learningConditions,
      moment().subtract(1 * 7, 'day'), // before second cycle starts
      entities,
    );

    // cycles dates as specified
    const { cycles } = entities;
    expect(cycles[0].start_date).toBe(cycleDates[0][0].format('YYYY-MM-DD'));
    expect(cycles[0].end_date).toBe(cycleDates[0][1].format('YYYY-MM-DD'));
    expect(cycles[1].start_date).toBe(cycleDates[1][0].format('YYYY-MM-DD'));
    expect(cycles[1].end_date).toBe(cycleDates[1][1].format('YYYY-MM-DD'));
    // Classroom report:
    // - extra participation shown in cycle 1 (replacement?)
  });

  // Participation after last cycle
  it('adds participation after last cycle to last cycle', () => {
    const cycleDates = [
      [moment().subtract(3 * 7 - 1, 'day'), moment().subtract(2 * 7, 'day')],
      [moment().subtract(2 * 7 - 1, 'day'), moment().subtract(1 * 7, 'day')],
    ];
    const entities = createTeamScenario(surveyLabel, learningConditions, {
      cycleDates,
    });

    // Fake additional participation after last cycle (which ended a week ago).
    fakeParticipation(
      surveyLabel,
      learningConditions,
      moment(), // today
      entities,
    );

    // Classroom report:
    // - extra participation show in cycle 2 (replacement?)
  });

  // Target group feature ON
  it('displays target group info', () => {
    const community = fakeCommunity();
    const targetGroupName = 'Foo Group';
    const { team, tritonParticipants } = createTeamScenario(
      surveyLabel,
      learningConditions,
      {
        community,
        numPerClassroom: 20,
        targetGroupName,
        ratioInTargetGroup: 0.5, // i.e. 10
      },
    );

    const byClass = groupBy(tritonParticipants, p => p.classroom_ids[0]);
    expect(team.target_group_name).toBe(targetGroupName);
    for (const cid of Object.keys(byClass)) {
      expect(byClass[cid].length).toBe(20);
      expect(byClass[cid].filter(p => p.in_target_group).length).toBe(10);
    }
    // Classroom report:
    // - ggplot shows target group data (7 levels of `subset_value`)
    // Community report:
    // - delta tables have target group column
  });

  // Improperly marked target group
  it('warns if target group flags set but group name is unset', () => {
    const { team } = createTeamScenario(surveyLabel, learningConditions, {
      numPerClassroom: 20,
      targetGroupName: null,
      ratioInTargetGroup: 0.5,
    });

    expect(team.target_group_name).toBe(null);
    // Classroom report:
    //  - ggplot has no target group info
    // -  warning that target group is unset
    // Team report:
    //  - ggplot has no target group info
    // -  warning that target group is unset
  });

  // Target group ON, no students marked
  it('warns when target group on, but no flags', () => {
    const entities = createTeamScenario(surveyLabel, learningConditions, {
      targetGroupName: 'Foo Group',
      ratioInTargetGroup: 0,
    });

    // Classroom report:
    //  - ggplot has no target group info
    // -  warning that no flags set
    // Team report:
    //  - ggplot has no target group info
    // -  warning that no flags set
  });

  // Low TG participation
  it('runs with adequate flags but low participation in target group', () => {
    const entities = createTeamScenario(surveyLabel, learningConditions, {
      includeParticipation: false,
      numPerClassroom: 20,
      targetGroupName: 'Foo Group',
      ratioInTargetGroup: 0.5,
    });
    const { cycles, tritonParticipants } = entities;

    // Fake participation, but remove most of the target group participants.
    const targetPpts = tritonParticipants.filter(p => p.in_target_group);
    const nonTargetPpts = tritonParticipants.filter(p => !p.in_target_group);
    fakeParticipation(
      surveyLabel,
      learningConditions,
      moment(cycles[0].end_date), // today
      {
        ...entities,
        tritonParticipants: nonTargetPpts.concat(targetPpts[0]),
      },
    );

    // ???
  });

  // Survey vars randomly missing data
  // ???

  // Missing open response data (one RU)
  // ???

  // Missing open response data (all RUs)
  it('??? no open response data', () => {
    const noOpenResponses = {
      or_cc1_1: null,
      or_cc2_1: null,
      or_cc3_1: null,
      or_c_belonging_classmates: null,
      or_c_belonging_teacher: null,
      or_c_belonging_thoughts: null,
      or_fg1_2: null,
      or_fg2_2: null,
      or_fg3_2: null,
      or_mw1_2: null,
      or_mw2_2: null,
      or_mw3_2: null,
      or_voice_choice: null,
      or_voice_idea: null,
      or_voice_suggestions: null,
      or_tc1_2: null,
      or_tc2_2: null,
      or_tc4_2: null,
    };
    const entities = createTeamScenario(surveyLabel, learningConditions, {
      numPerClassroom: 20,
      answerProfiles: [[20, noOpenResponses]],
    });

    // Classroom report:
    // - no open responses
  });

  // Missing LC data for roster
  it('adjusts graphs when no lc data', () => {
    const noClassroomBelonging = {
      c_belonging_classmates: null,
      c_belonging_teacher: null,
      c_belonging_thoughts: null,
    };
    const entities = createTeamScenario(surveyLabel, learningConditions, {
      numPerClassroom: 20,
      answerProfiles: [[20, noClassroomBelonging]],
    });

    // Classroom report:
    // - no cross section graph for missing lc
    // - bar graph for missing lc has previous cycle as final time point
  });

  // Missing demographics (project-wide)
  it('hides race bar if no one answers race question', () => {
    const noRace = {
      race_amindian: null,
      race_e_asian: null,
      race_se_asian: null,
      race_s_asian: null,
      race_o_asian: null,
      race_mex: null,
      race_puerto_rican: null,
      race_central_american: null,
      race_o_latino: null,
      race_african_american: null,
      race_african: null,
      race_caribbean: null,
      race_o_black: null,
      race_european: null,
      race_middle_eastern: null,
      race_o_white: null,
      race_pac_isl: null,
      race_other: null,
      race_text: null,
      race_other: null,
    };
    const entities = createTeamScenario(surveyLabel, learningConditions, {
      numPerClassroom: 20,
      answerProfiles: [[20, noRace]],
    });

    // Classroom report:
    // - doesn't show race bar
    // Team report:
    // - doesn't show race bar
  });

  // Small cell sizes: gender
  it('obscures small cell for gender', () => {
    const male = { gender: 'male' };
    const female = { gender: 'female' };

    const entities = createTeamScenario(surveyLabel, learningConditions, {
      numPerClassroom: 20,
      numCycles: 2,
      includeParticipation: false,
    });
    const { cycles } = entities;

    // Cycle 1
    const { neptuneParticipants } = fakeParticipation(
      surveyLabel,
      learningConditions,
      moment(cycles[0].end_date),
      entities,
      {
        answerProfiles: [[3, male], [17, female]],
      },
    );

    // Cycle 2
    fakeParticipation(
      surveyLabel,
      learningConditions,
      moment(cycles[1].end_date),
      { ...entities, neptuneParticipants },
    );
  });

  // Small cell sizes: target groups
  it('warns when target group on, but too few flags', () => {
    const entities = createTeamScenario(surveyLabel, learningConditions, {
      numPerClassroom: 20,
      targetGroupName: 'Foo Group',
      ratioInTargetGroup: 0.15, // i.e. 3, which is < 5, min cell size
    });

    // Classroom report:
    // - ggplot has no target group info
    // - warning that too few flags set
    // Team report:
    // - ggplot has no target group info
    // - warning that too few flags set
  });

  // see scenarios.test.js for examples of actually running RServe
});

// These tests use an out-of-date mocking api, but are useful to document all
// the things we have the ambition to test.
describe.skip('C-SET', () => {
  const surveyLabel = 'beleset19';
  const learningConditions = [
    'identity-threat',
    'institutional-growth-mindset',
    'self-efficacy',
    'social-belonging',
    'trust-fairness',
  ];

  // Small cell sizes: financial stress
  it('obscures small cell for financial stress', () => {
    const secure = {
      food_insecure: 3, // "I worried whether my food would run out" -> never
      fin_insecure_1: null, // same as not checking the box, NA in R
      fin_insecure_2: null,
      fin_insecure_3: null,
      fin_insecure_4: null,
    };
    const insecure = {
      food_insecure: 1, // "I worried whether my food would run out" -> often
    };

    const entities = createTeamScenario(surveyLabel, learningConditions, {
      numPerClassroom: 20,
      numCycles: 2,
      includeParticipation: false,
    });
    const { cycles } = entities;

    // Cycle 1
    const { neptuneParticipants } = fakeParticipation(
      surveyLabel,
      learningConditions,
      moment(cycles[0].end_date),
      entities,
      {
        answerProfiles: [[3, secure], [17, insecure]],
      },
    );

    // Cycle 2
    fakeParticipation(
      surveyLabel,
      learningConditions,
      moment(cycles[1].end_date),
      { ...entities, neptuneParticipants },
    );
  });
});

// These tests use an out-of-date mocking api, but are useful to document all
// the things we have the ambition to test.
describe.skip('structure of datasets needed by template', () => {
  const dataset = fakeClassDataset();
  describe('class report', classReportTemplateRequirements(dataset));
});
