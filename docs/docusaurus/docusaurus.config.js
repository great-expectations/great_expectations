/** @type {import('@docusaurus/types').DocusaurusConfig} */

const remarkNamedSnippets = require('./scripts/remark-named-snippets/index')
const remarkCodeImport = require('remark-code-import')

module.exports = {
  title: 'Great Expectations',
  tagline: 'Always know what to expect from your data.',
  url: 'https://docs.greatexpectations.io', // Url to your site with no trailing slash
  baseUrl: '/',
  onBrokenLinks: 'throw',
  onBrokenMarkdownLinks: 'warn',
  favicon: '/img/gx-mark.png',
  organizationName: 'great-expectations',
  projectName: 'great_expectations',
  plugins: [
    '@docusaurus-terminology/parser',
    'docusaurus-plugin-sass',
    [
      require.resolve('docusaurus-gtm-plugin'),
      {
        id: 'GTM-K63L45F' // GTM Container ID
      }
    ]
  ],

  themeConfig: {
    algolia: {
      appId: 'PFK639M3JK', 
      apiKey: 'fc3e3b1588b46d8d476aca9c1cadd53f',
      indexName: 'greatexpectations',
      searchPagePath: 'search',
      searchParameters: {
        facetFilters: [
        'version:current'
      ]
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
    image: 'img/gx-preview.png',
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
          type: 'docsVersionDropdown',
          position: 'left',
          dropdownItemsAfter: [
            {
              to: 'https://legacy.docs.greatexpectations.io/',
              label: '0.13.x and earlier'
            }
          ],
          dropdownActiveClassDisabled: true
        },
        {
          label: 'Product',
          position: 'right',
          items: [
            {
              label: 'GX CLOUD',
              to: 'https://greatexpectations.io/gx-cloud'
            },
            {
              label: 'GX OSS',
              to: 'https://greatexpectations.io/gx-oss'
            }
          ]
        },
        {
          label: 'Community',
          position: 'right',
          items: [
            {
              label: 'COMMUNITY HOME',
              to: 'https://greatexpectations.io/community'
            },
            {
              label: 'SLACK',
              to: 'https://greatexpectations.io/slack'
            },
            {
              label: 'GITHUB',
              to: 'https://github.com/great-expectations/great_expectations'
            },
            {
              label: 'JOIN THE EMAIL LIST',
              to: 'https://greatexpectations.io/newsletter'
            }
          ]
        },
        {
          label: 'RESOURCES',
          position: 'right',
          items: [
            {
              label: 'INTEGRATIONS',
              to: 'https://greatexpectations.io/integrations'
            },
            {
              label: 'DOCUMENTATION',
              to: 'https://docs.greatexpectations.io/docs/'
            },
            {
              label: 'EXPECTATION GALLERY',
              to: 'https://greatexpectations.io/expectations'
            },
            {
              label: 'GREAT EXPECTATIONS BLOG',
              to: 'https://greatexpectations.io/blog'
            },
            {
              label: 'GREAT EXPECTATIONS CASE STUDIES',
              to: 'https://greatexpectations.io/case-studies'
            }
          ]
        },
        {
          label: 'Company',
          position: 'right',
          items: [
            {
              label: 'ABOUT US',
              to: 'https://greatexpectations.io/company'
            },
            {
              label: 'CAREERS',
              to: 'https://jobs.greatexpectations.io/'
            }
          ]
        },
        {
          to: 'https://greatexpectations.io/gx-cloud',
          label: 'GX Cloud',
          position: 'right',
          className: 'header-cloud-link',
          'aria-label': 'Early cloud access'
        }
      ]
    },
    footer: {
      style: 'light',
      logo: {
        alt: 'Great Expectations',
        src: 'img/gx-logo-dark.svg',
        href: 'https://greatexpectations.io',
        width: '100%',
        height: 'auto'
      },
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
          title: 'Company',
          items: [
            {
              label: 'Careers',
              to: 'https://jobs.greatexpectations.io/'
            },
            {
              label: 'DPA',
              to: 'https://greatexpectations.io/pdf/dpa'
            },
            {
              label: 'Master Subscription Agreement',
              to: 'https://greatexpectations.io/pdf/msa'
            },
            {
              label: 'Privacy Policy',
              to: 'https://greatexpectations.io/privacy-policy'
            }
          ]
        },
        {
          title: 'Check Us Out',
          items: [
            {
              html: `
                <a class="footer__icon" href="https://greatexpectations.io/slack" target="_blank" rel="noreferrer noopener" aria-label="check out or Slack community">
                  <svg stroke="currentColor" fill="currentColor" stroke-width="0" viewBox="0 0 1024 1024" height="18px" width="18px" xmlns="http://www.w3.org/2000/svg"><path d="M409.4 128c-42.4 0-76.7 34.4-76.7 76.8 0 20.3 8.1 39.9 22.4 54.3 14.4 14.4 33.9 22.5 54.3 22.5h76.7v-76.8c0-42.3-34.3-76.7-76.7-76.8zm0 204.8H204.7c-42.4 0-76.7 34.4-76.7 76.8s34.4 76.8 76.7 76.8h204.6c42.4 0 76.7-34.4 76.7-76.8.1-42.4-34.3-76.8-76.6-76.8zM614 486.4c42.4 0 76.8-34.4 76.7-76.8V204.8c0-42.4-34.3-76.8-76.7-76.8-42.4 0-76.7 34.4-76.7 76.8v204.8c0 42.5 34.3 76.8 76.7 76.8zm281.4-76.8c0-42.4-34.4-76.8-76.7-76.8S742 367.2 742 409.6v76.8h76.7c42.3 0 76.7-34.4 76.7-76.8zm-76.8 128H614c-42.4 0-76.7 34.4-76.7 76.8 0 20.3 8.1 39.9 22.4 54.3 14.4 14.4 33.9 22.5 54.3 22.5h204.6c42.4 0 76.7-34.4 76.7-76.8.1-42.4-34.3-76.7-76.7-76.8zM614 742.4h-76.7v76.8c0 42.4 34.4 76.8 76.7 76.8 42.4 0 76.8-34.4 76.7-76.8.1-42.4-34.3-76.7-76.7-76.8zM409.4 537.6c-42.4 0-76.7 34.4-76.7 76.8v204.8c0 42.4 34.4 76.8 76.7 76.8 42.4 0 76.8-34.4 76.7-76.8V614.4c0-20.3-8.1-39.9-22.4-54.3-14.4-14.4-34-22.5-54.3-22.5zM128 614.4c0 20.3 8.1 39.9 22.4 54.3 14.4 14.4 33.9 22.5 54.3 22.5 42.4 0 76.8-34.4 76.7-76.8v-76.8h-76.7c-42.3 0-76.7 34.4-76.7 76.8z"></path></svg>
                </a>
                <a class="footer__icon" href="https://twitter.com/expectgreatdata" target="_blank" rel="noreferrer noopener" aria-label="check out or Slack community">
                  <svg stroke="currentColor" fill="currentColor" stroke-width="0" viewBox="0 0 1024 1024" height="18px" width="18px" xmlns="http://www.w3.org/2000/svg"><path d="M928 254.3c-30.6 13.2-63.9 22.7-98.2 26.4a170.1 170.1 0 0 0 75-94 336.64 336.64 0 0 1-108.2 41.2A170.1 170.1 0 0 0 672 174c-94.5 0-170.5 76.6-170.5 170.6 0 13.2 1.6 26.4 4.2 39.1-141.5-7.4-267.7-75-351.6-178.5a169.32 169.32 0 0 0-23.2 86.1c0 59.2 30.1 111.4 76 142.1a172 172 0 0 1-77.1-21.7v2.1c0 82.9 58.6 151.6 136.7 167.4a180.6 180.6 0 0 1-44.9 5.8c-11.1 0-21.6-1.1-32.2-2.6C211 652 273.9 701.1 348.8 702.7c-58.6 45.9-132 72.9-211.7 72.9-14.3 0-27.5-.5-41.2-2.1C171.5 822 261.2 850 357.8 850 671.4 850 843 590.2 843 364.7c0-7.4 0-14.8-.5-22.2 33.2-24.3 62.3-54.4 85.5-88.2z"></path></svg>
                </a>
                <a class="footer__icon" href="https://github.com/great-expectations/great_expectations" target="_blank" rel="noreferrer noopener" aria-label="check out or Slack community">
                  <svg stroke="currentColor" fill="currentColor" stroke-width="0" viewBox="0 0 1024 1024" height="18px" width="18px" xmlns="http://www.w3.org/2000/svg"><path d="M511.6 76.3C264.3 76.2 64 276.4 64 523.5 64 718.9 189.3 885 363.8 946c23.5 5.9 19.9-10.8 19.9-22.2v-77.5c-135.7 15.9-141.2-73.9-150.3-88.9C215 726 171.5 718 184.5 703c30.9-15.9 62.4 4 98.9 57.9 26.4 39.1 77.9 32.5 104 26 5.7-23.5 17.9-44.5 34.7-60.8-140.6-25.2-199.2-111-199.2-213 0-49.5 16.3-95 48.3-131.7-20.4-60.5 1.9-112.3 4.9-120 58.1-5.2 118.5 41.6 123.2 45.3 33-8.9 70.7-13.6 112.9-13.6 42.4 0 80.2 4.9 113.5 13.9 11.3-8.6 67.3-48.8 121.3-43.9 2.9 7.7 24.7 58.3 5.5 118 32.4 36.8 48.9 82.7 48.9 132.3 0 102.2-59 188.1-200 212.9a127.5 127.5 0 0 1 38.1 91v112.5c.8 9 0 17.9 15 17.9 177.1-59.7 304.6-227 304.6-424.1 0-247.2-200.4-447.3-447.5-447.3z"></path></svg>
                </a>
                <a class="footer__icon" href="https://www.linkedin.com/company/greatexpectations-data" target="_blank" rel="noreferrer noopener" aria-label="check out or Slack community">
                  <svg stroke="currentColor" fill="currentColor" stroke-width="0" viewBox="0 0 1024 1024" height="18px" width="18px" xmlns="http://www.w3.org/2000/svg"><path d="M847.7 112H176.3c-35.5 0-64.3 28.8-64.3 64.3v671.4c0 35.5 28.8 64.3 64.3 64.3h671.4c35.5 0 64.3-28.8 64.3-64.3V176.3c0-35.5-28.8-64.3-64.3-64.3zm0 736c-447.8-.1-671.7-.2-671.7-.3.1-447.8.2-671.7.3-671.7 447.8.1 671.7.2 671.7.3-.1 447.8-.2 671.7-.3 671.7zM230.6 411.9h118.7v381.8H230.6zm59.4-52.2c37.9 0 68.8-30.8 68.8-68.8a68.8 68.8 0 1 0-137.6 0c-.1 38 30.7 68.8 68.8 68.8zm252.3 245.1c0-49.8 9.5-98 71.2-98 60.8 0 61.7 56.9 61.7 101.2v185.7h118.6V584.3c0-102.8-22.2-181.9-142.3-181.9-57.7 0-96.4 31.7-112.3 61.7h-1.6v-52.2H423.7v381.8h118.6V604.8z"></path></svg>
                </a>
              `
            }
          ]
        }
      ],
      copyright: `Copyright © ${new Date().getFullYear()} Great Expectations. All Rights Reserved.`
    }
  },

  // themes:[ ],
  presets: [
    [
      '@docusaurus/preset-classic',
      {
        docs: {
          sidebarPath: require.resolve('./sidebars.js'),
          // Note: remarkCodeImport is included to handle earlier versions with line number references (e.g. v0.14.13)
          remarkPlugins: [remarkNamedSnippets, remarkCodeImport],
          lastVersion: 'current',
          versions: {
            current: {
              label: '0.17.4',
              path: ''
            }
          }
        },
        theme: {
          customCss: require.resolve('./src/css/custom.scss')
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
