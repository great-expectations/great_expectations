import React from 'react';
import DocSidebarItems from '@theme-original/DocSidebarItems';
import {useNavbarMobileSidebar} from "@docusaurus/theme-common/internal";

export default function DocSidebarItemsWrapper(props) {
    const mobileSidebar = useNavbarMobileSidebar();
    const handleMobileDocSidebarItemClick = (item) => {
        if (item.type === 'link') {
            mobileSidebar.toggle();
        }
    }
  return (
      <DocSidebarItems {...props} onItemClick={props.onItemClick ? handleMobileDocSidebarItemClick : null} />
  );
}
