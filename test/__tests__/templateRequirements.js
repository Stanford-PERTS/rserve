const shortUidRegex = /^[A-Za-z][A-Za-z0-9]{7,19}$/;
const filenameRegex = /^\d{4}-\d{2}-\d{2}\.html$/;

const learningConditionLabels = [
  'feedback-for-growth',
  'teacher-caring',
  'meaningful-work',
  'cultural-competence',
  'student-voice',
  'classroom-belonging',
  'social-belonging',
  'trust-fairness',
  'institutional-gms',
  'stem-efficacy',
  'identity-threat',
];

export const fakeClassDataset = () => ({
  classroom_id_url: 'pBR6b1E6AKmij2qN', // short uid
  classroom_name: 'Goldenkranz, P1 APES', // string
  date_of_earliest_uncycled_data: null, // null | string,
  fidelity_honest: 96, // number
  fidelity_tuq: 81, // number
  filename: '2019-12-16.html', // YYYY-MM-DD.html
  id: 'Classroom_pBR6b1E6AKmij2qN', // long uid
  learning_conditions: [
    {
      active: true, // boolean
      bar_chart_64: '...', // string, optional
      bar_chart_data: {},
      label: 'feedback-for-growth', // enum
      timeline_active: true, // boolean
      timeline_chart_64: '...', // string, optional
    },
  ],
  max_cycles_missing: 35, // number
  no_cycles_defined: false, // boolean
  open_responses: {
    or_1: ['I feel like...'],
    or_2: [],
  },
  page_title: 'Goldenkranz, P1 APES', // string
  // string[] length 3
  participation_table_columns: [
    'Cycle',
    'Cupertino Skillful Teacher',
    'Goldenkranz, P1 APES',
  ],
  participation_table_data: [
    // string[] length 3
    ['Cycle 01 (Sep. 2 - Sep. 22)', '509 (95%)', '27 (90%)'],
    ['Cycle 02 (Sep. 23 - Oct. 27)', '412 (77%)', '0 (0%)'],
  ],
  report_date: 'December 16, 2019', // string
  ru_expected_n: 30, // number
  target_msg: 'Your team’s target group is “Focal Students”.', // string
  team_id: 'n2ZmivErsicDZHdR', // short uid
  team_name: 'Cupertino Skillful Teacher', // string
  zero_length_table: false, // boolean
});

// Example: describe('my tests', classReportTemplateRequirements(dataset));
export const classReportTemplateRequirements = dataset => () => {
  const d = dataset;

  it('has url params', () => {
    expect(d.classroom_id_url).toMatch(shortUidRegex);
    expect(d.team_id).toMatch(shortUidRegex);
  });

  it('has display strings', () => {
    expect(d.classroom_name).toBeNonEmptyString();
    expect(d.date_of_earliest_uncycled_data).toBeNonEmptyStringOr(null);
    expect(d.page_title).toBeNonEmptyString();
    expect(d.report_date).toBeNonEmptyString();
    expect(d.target_msg).toBeNonEmptyString();
    expect(d.team_name).toBeNonEmptyString();
  });

  it('has display numbers', () => {
    expect(d.fidelity_honest).toBeNonNegativeNumber();
    expect(d.fidelity_tuq).toBeNonNegativeNumber();
    expect(d.max_cycles_missing).toBeNonNegativeNumber();
    expect(d.ru_expected_n).toBeNonNegativeNumber();
  });

  it('has booleans', () => {
    expect(typeof d.no_cycles_defined).toBe('boolean');
    expect(typeof d.zero_length_table).toBe('boolean');
  });

  it('has filename', () => {
    expect(d.filename).toMatch(filenameRegex);
  });

  it('has learning conditions', () => {
    expect(d.learning_conditions).toBeInstanceOf(Array);
    expect(d.learning_conditions.length).toBeGreaterThan(0);

    for (const lc of d.learning_conditions) {
      expect(typeof lc.active).toBe('boolean');

      if (lc.bar_chart_64 !== undefined) {
        expect(lc.bar_chart_64).toBeNonEmptyString();
      }
      if (lc.timeline_chart_64 !== undefined) {
        expect(lc.timeline_chart_64).toBeNonEmptyString();
      }

      expect(learningConditionLabels).toContain(lc.label);
      expect(typeof lc.timeline_active).toBe('boolean');
    }
  });

  it('has open responses', () => {
    expect(d.open_responses).not.toBe(null);
    expect(typeof d.open_responses).toBe('object');

    // Open responses may be an empty object, and each entry may be an
    // empty array.

    for (const [questionId, responses] of Object.entries(d.open_responses)) {
      expect(questionId).toBeNonEmptyString();
      expect(responses).toBeInstanceOf(Array);
      for (const r of responses) {
        expect(r).toBeNonEmptyString();
      }
    }
  });

  it('has participation table data', () => {
    expect(d.participation_table_columns).toBeInstanceOf(Array);
    // 3 columns: cycle labels, team data, class data
    expect(d.participation_table_columns.length).toBe(3);

    expect(d.participation_table_data).toBeInstanceOf(Array);
    for (const row of d.participation_table_data) {
      expect(row).toBeInstanceOf(Array);
      expect(row.length).toBe(3);
    }
  });
};
