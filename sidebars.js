module.exports = {
  docs: [
    'intro',
    {
      type: 'category',
      label: 'Getting Started (A Tutorial)',
      items: [
        { type: 'doc', id: 'tutorials/getting_started/tutorial_overview', label: 'Overview' },
        { type: 'doc', id: 'tutorials/getting_started/tutorial_setup', label: '1. Setup' },
        { type: 'doc', id: 'tutorials/getting_started/tutorial_connect_to_data', label: '2. Connect to Data' },
        { type: 'doc', id: 'tutorials/getting_started/tutorial_create_expectations', label: '3. Create Expectations' },
        { type: 'doc', id: 'tutorials/getting_started/tutorial_validate_data', label: '4. Validate Data' },
        { type: 'doc', id: 'tutorials/getting_started/tutorial_review', label: 'Review and next steps' }
      ]
    },
    {
      type: 'category',
      label: 'Step 1: Setup',
      items: [
        { type: 'doc', id: 'guides/setup/setup_overview', label: 'Overview' },
        {
          type: 'category',
          label: 'How-to guides',
          items: []
        }
      ]
    },
    {
      type: 'category',
      label: 'Step 2: Connect to data',
      items: [
        { type: 'doc', id: 'guides/connecting_to_your_data/connect_to_data_overview', label: 'Overview' },
        {
          type: 'category',
          label: 'How-to guides',
          items: []
        }
      ]
    },
    {
      type: 'category',
      label: 'Step 3: Create Expectations',
      items: [
        { type: 'doc', id: 'guides/expectations/create_expectations_overview', label: 'Overview' },
        {
          type: 'category',
          label: 'How-to guides',
          items: []
        }
      ]
    },
    {
      type: 'category',
      label: 'Step 4: Validate data',
      items: [
        { type: 'doc', id: 'guides/validation/validate_data_overview', label: 'Overview' },
        {
          type: 'category',
          label: 'How-to guides',
          items: []
        }
      ]
    },
    {
      type: 'category',
      label: 'Reference Architectures',
      items: []
    },
    {
      type: 'category',
      label: 'Contributing',
      items: []
    },
    {
      type: 'category',
      label: 'Reference',
      items: []
    },
    {
      type: 'category',
      label: 'Updates and migration',
      items: []
    }
  ]
}
