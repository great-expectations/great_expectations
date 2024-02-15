import React from 'react';
import DocSidebar from '@theme-original/DocSidebar';
import { useLocation } from '@docusaurus/router';

export default function DocSidebarWrapper(props) {
  const { pathname, hash} = useLocation();

  let path = pathname;
  if (hash) {
      path = path + hash;
  }

  return (
    <DocSidebar {...props} path={path}/>
  );
}
