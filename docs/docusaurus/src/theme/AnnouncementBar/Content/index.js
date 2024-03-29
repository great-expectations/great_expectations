import React from 'react';
import clsx from 'clsx';
import {useThemeConfig} from '@docusaurus/theme-common';
import styles from './styles.module.css';
export default function AnnouncementBarContent(props) {
  const {announcementBar} = useThemeConfig();
  const {content} = announcementBar;
  return (
    <div
      {...props}
      className={clsx(styles.content, props.className)}
      // Developer provided the HTML, so assume it's safe.
      // eslint-disable-next-line react/no-danger
      dangerouslySetInnerHTML={{__html: content}}
    />
  );
}
