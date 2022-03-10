/** @type {import('@docusaurus/types').DocusaurusConfig} */

const remarkCodeImport = require('remark-code-import')

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
    prism: {
      theme: require('prism-react-renderer/themes/vsDark')
    },
    colorMode: {
      disableSwitch: true
    },
    gtag: {
      // You can also use your "G-" Measurement ID here.
      trackingID: 'UA-138955219-1',
      // Optional fields.
      anonymizeIP: true // Should IPs be anonymized?
    },
    zoomSelector: '.markdown :not(em) > img',
    announcementBar: {
      id: 'RTD_docs', // Link to RTD Docs
      content:
                '🔄 Older Documentation for Great Expectations can be found at the <a href="https://legacy.docs.greatexpectations.io">legacy.docs.greatexpectations.io</a> 🔄',
      // backgroundColor: '#32a852', // Defaults to `#fff`.
      backgroundColor: '#143556', // Defaults to `#fff`.
      textColor: '#ffffff', // Defaults to `#000`.
      isCloseable: false // Defaults to `true`.
    },
    navbar: {
      logo: {
        alt: 'Great Expectations',
        src: 'img/great-expectations-logo-full-size.png',
        href: 'https://greatexpectations.io'
      },
      items: [
        {
          label: 'Community',
          position: 'right',
          items: [
            {
              label: 'Slack',
              href: 'https://greatexpectations.io/slack'
            },
            {
              label: 'Github',
              href: 'https://github.com/great-expectations/great_expectations'
            },
            {
              label: 'Discuss',
              href: 'https://discuss.greatexpectations.io/'
            },
            {
              label: 'Newsletter',
              href: 'https://greatexpectations.io/newsletter'
            }
          ]
        },
        {
          label: 'Expectations',
          position: 'right',
          href: 'https://greatexpectations.io/expectations'
        },
        {
          label: 'Documentation',
          position: 'right',
          items: [
            {
              label: 'V2 Documentation',
              href: 'https://legacy.docs.greatexpectations.io/en/latest'
              // activeBasePath: 'docs',
            }
          ]
        },
        {
          label: 'Case Studies',
          position: 'right',
          href: 'https://greatexpectations.io/case-studies'
        },
        {
          label: 'Blog',
          position: 'right',
          href: 'https://greatexpectations.io/blog'
        }
      ]
    },
    footer: {
      style: 'light',
      links: [
        {
          title: 'Docs',
          items: [
            {
              label: 'Getting Started',
              to: 'docs/tutorials/getting_started/tutorial_overview'
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
              label: 'Discuss',
              href: 'https://discuss.greatexpectations.io/'
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
          remarkPlugins: [remarkCodeImport],
          editUrl:
                        'https://github.com/great-expectations/great_expectations/tree/develop/'
        },
        theme: {
          customCss: require.resolve('./src/css/custom.css')
        },
        lastVersion: 'current',
        versions: {
          // Example configuration:
          // <WILL> may have to be fixed
          current: {
            label: 'docs',
            path: 'docs'
          },
          '0.13.9': {
            label: '0.13.9-docs',
            path: '0.13.9'
          }
        }
      }
    ]
  ]
}
