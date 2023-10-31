import React, { useState } from 'react';
import clsx from 'clsx';
import { ThemeClassNames, useThemeConfig } from '@docusaurus/theme-common';
import DocSidebarItems from '@theme/DocSidebarItems';
import styles from './styles.module.css';
export default function DocSidebarDesktopContent({ path, sidebar, className }) {
return (
    <>
      <nav>
        <ul className={clsx(ThemeClassNames.docs.docSidebarMenu, 'menu__list')}>
          <DocSidebarItems items={sidebar} activePath={path} level={1} />
        </ul>
        {!path.indexOf('/software') > -1 && (
          <ul className={clsx(ThemeClassNames.docs.docSidebarMenu, 'menu__list', styles.menu__listBottom)}>
            <li>
              <a href="https://status.greatexpectations.io/?referral=docs-sidebar">GX Cloud status</a>
            </li>
          </ul>
        )}
      </nav>
    </>
  );
}