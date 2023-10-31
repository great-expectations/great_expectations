import React, { useState } from 'react';
import clsx from 'clsx';
import { ThemeClassNames, useThemeConfig } from '@docusaurus/theme-common';
import {
  useAnnouncementBar,
  useScrollPosition,
} from '@docusaurus/theme-common/internal';
import DocSidebarItems from '@theme/DocSidebarItems';
import SelectNav from '@site/src/components/SelectNav';
import styles from './styles.module.css';
function useShowAnnouncementBar() {
  const { isActive } = useAnnouncementBar();
  const [showAnnouncementBar, setShowAnnouncementBar] = useState(isActive);
  useScrollPosition(
    ({ scrollY }) => {
      if (isActive) {
        setShowAnnouncementBar(scrollY === 0);
      }
    },
    [isActive],
  );
  return isActive && showAnnouncementBar;
}
function useSoftwareNavItems() {
  // TODO temporary casting until ThemeConfig type is improved
  return useThemeConfig().softwareNav.items;
}
export default function DocSidebarDesktopContent({ path, sidebar, className }) {
  const showAnnouncementBar = useShowAnnouncementBar();
  const softwareNavItems = useSoftwareNavItems();
  return (
    <>
      {path.indexOf('/software') > -1 && (
        <SelectNav items={softwareNavItems} label="Select Software Version" />
      )}
      <nav
        className={clsx(
          'menu thin-scrollbar',
          styles.menu,
          showAnnouncementBar && styles.menuWithAnnouncementBar,
          className,
        )}>
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