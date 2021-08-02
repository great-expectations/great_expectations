/** @type {import('@docusaurus/types').DocusaurusConfig} */

const remarkCodeImport = require('remark-code-import')

module.exports = {
  title: 'Great Expectations',
  tagline: 'Always know what to expect from your data.',
  // TODO update for proper hosting URL once decisions are made
  url: 'https://knoxpod.netlify.com', // Url to your site with no trailing slash
  baseUrl: '/',
  onBrokenLinks: 'throw',
  onBrokenMarkdownLinks: 'warn',
  favicon: 'img/favicon.ico',
  organizationName: 'great-expectations',
  projectName: 'great_expectations',
  plugins: [
    'plugin-image-zoom'
  ],
  themeConfig: {
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
    //     '🔄 Older Documentation for Great Expectations can be found at the <a href="file:///Users/work/Development/great_expectations/docs_rtd/build/html/index.html">Read the Docs site</a> 🔄',
    //   backgroundColor: '#32a852', // Defaults to `#fff`.
    //   textColor: '#091E42', // Defaults to `#000`.
    //   isCloseable: false, // Defaults to `true`.
    // },
    navbar: {
      title: 'great_expectations',
      logo: {
        alt: 'Great Expectations',
        src: 'img/logo.svg'
      },
      items: [
        {
          label: 'Documentation',
          position: 'right',
          items: [
            {
              label: '0.13.10',
              to: 'docs/'
              // activeBasePath: 'docs',
            },
            {
              label: '0.13.9',
              to: 'docs/'
              // activeBasePath: 'docs',
            },
            {
              label: '0.13.8',
              href: 'https://docs.greatexpectations.io/en/0.13.8/'
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
        },

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
        }

      ]
    },
    footer: {
      style: 'dark',
      links: [
        {
          title: 'Docs',
          items: [
            {
              label: 'Getting Started',
              to: 'docs/'
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
