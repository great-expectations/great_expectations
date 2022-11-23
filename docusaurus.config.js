/** @type {import('@docusaurus/types').DocusaurusConfig} */

const remarkCodeImport = require('remark-code-import')
const remarkNamedSnippets = require('./docs/scripts/remark-named-snippets/index')

module.exports = {
  title: 'Great Expectations',
  tagline: 'Always know what to expect from your data.',
  url: 'https://docs.greatexpectations.io', // Url to your site with no trailing slash
  baseUrl: '/',
  onBrokenLinks: 'throw',
  onBrokenMarkdownLinks: 'warn',
  favicon: 'https://greatexpectations.io/favicon-32x32.png',
  organizationName: 'great-expectations',
  projectName: 'great_expectations',
  plugins: [
    // ["plugin-image-zoom"],
    require.resolve('@cmfcmf/docusaurus-search-local'),
    '@docusaurus-terminology/parser'
  ],

  themeConfig: {
    algolia: {
      // If Algolia did not provide you any appId, use 'BH4D9OD16A'
      appId: 'B4HD9FJQCB',

      // Public API key: it is safe to commit it
      apiKey: '16dae2c1fabc515311cada8ace06060a',

      indexName: 'docs-greatexpectations',
      searchPagePath: 'search'

      // Optional: see doc section below
      // contextualSearch: true,

      // Optional: Specify domains where the navigation should occur through window.location instead on history.push. Useful when our Algolia config crawls multiple documentation sites and we want to navigate with window.location.href to them.
      // externalUrlRegex: 'external\\.com|domain\\.com',

      // Optional: see doc section below
      // appId: 'YOUR_APP_ID',

      // Optional: Algolia search parameters
      // searchParameters: {},

      // ... other Algolia params
    },
    prism: {
      theme: require('prism-react-renderer/themes/vsDark')
    },
    colorMode: {
      disableSwitch: true
    },
    zoomSelector: '.markdown :not(em) > img',
    // announcementBar: {
    //   id: 'RTD_docs', // Link to RTD Docs
    //   content:
    //             '🔄 Older Documentation for Great Expectations can be found at the <a href="https://legacy.docs.greatexpectations.io">legacy.docs.greatexpectations.io</a> 🔄',
    //   // backgroundColor: '#32a852', // Defaults to `#fff`.
    //   backgroundColor: '#143556', // Defaults to `#fff`.
    //   textColor: '#ffffff', // Defaults to `#000`.
    //   isCloseable: false // Defaults to `true`.
    // },
    navbar: {
      logo: {
        alt: 'Great Expectations',
        src: 'img/gx-logo.svg',
        href: 'https://greatexpectations.io'
      },
      items: [
        {
          type: 'search',
          position: 'right'
        },
        {
          label: 'Product',
          position: 'right',
          items: [
            {
              label: 'GX ClOUD',
              href: 'https://greatexpectations.io/gx-cloud'
            },
            {
              label: 'GX OSS',
              href: 'https://greatexpectations.io/gx-oss'
            }
          ]
        },
        {
          label: 'Community',
          position: 'right',
          items: [
            {
              label: 'COMMUNITY HOME',
              href: 'https://greatexpectations.io/community'
            },
            {
              label: 'SLACK',
              href: 'https://greatexpectations.io/slack'
            },
            {
              label: 'GITHUB',
              href: 'https://github.com/great-expectations/great_expectations'
            },
            {
              label: 'NEWSLETTER',
              href: 'https://greatexpectations.io/newsletter'
            }
          ]
        },
        {
          label: 'RESOURCES',
          position: 'right',
          items: [
            {
              label: 'INTEGRATIONS',
              href: 'https://greatexpectations.io/integrations'
            },
            {
              label: 'DOCUMENTATION',
              href: 'https://docs.greatexpectations.io/docs/'
            },
            {
              label: 'EXPECTATION GALLERY',
              href: 'https://greatexpectations.io/expectations'
            },
            {
              label: 'GREAT EXPECTATIONS BLOG',
              href: 'https://greatexpectations.io/blog'
            },
            {
              label: 'GREAT EXPECTATIONS CASE STUDIES',
              href: 'https://greatexpectations.io/case-studies'
            }
          ]
        },
        {
          label: 'Company',
          position: 'right',
          items: [
            {
              label: 'ABOUT US',
              href: 'https://greatexpectations.io/company'
            },
            {
              label: 'CAREERS',
              href: 'https://greatexpectations.io/case-studies'
            }
          ]
        },
        {
          href: 'https://greatexpectations.io/cloud',
          label: 'Early cloud access',
          position: 'right',
          className: 'header-cloud-link',
          'aria-label': 'Early cloud access'
        }
      ]
    },
    footer: {
      style: 'light',
      links: [
        {
          title: 'Product',
          items: [
            {
              label: 'GX Cloud',
              to: 'https://greatexpectations.io/gx-cloud'
            },
            {
              label: 'GX OSS',
              to: 'https://greatexpectations.io/gx-oss'
            }
          ]
        },
        {
          title: 'Community',
          items: [
            {
              label: 'Slack',
              href: 'https://greatexpectations.io/slack'
            },
            {
              label: 'Discussions',
              href: 'https://github.com/great-expectations/great_expectations/discussions/'
            },
            {
              label: 'Twitter',
              href: 'https://twitter.com/expectgreatdata'
            },
            {
              label: 'YouTube',
              href: 'https://www.youtube.com/c/GreatExpectationsData'
            }
          ]
        }
      ],
      copyright: `Copyright © ${new Date().getFullYear()} Superconductive.`
    }
  },

  // themes:[ ],
  presets: [
    [
      '@docusaurus/preset-classic',
      {
        docs: {
          sidebarPath: require.resolve('./sidebars.js'),
          remarkPlugins: [remarkCodeImport, remarkNamedSnippets],
          editUrl:
                        'https://github.com/great-expectations/great_expectations/tree/develop/'
        },
        theme: {
          customCss: require.resolve('./src/css/custom.css')
        },
        gtag: {
          // You can also use your "G-" Measurement ID here.
          trackingID: 'UA-138955219-1',
          // Optional fields.
          anonymizeIP: true // Should IPs be anonymized?
        }
      }
    ]
  ]
}
