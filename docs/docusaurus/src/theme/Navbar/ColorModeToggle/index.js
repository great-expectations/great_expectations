import React from 'react';
import ColorModeToggle from '@theme-original/Navbar/ColorModeToggle';
import styles from './styles.module.scss';

export default function ColorModeToggleWrapper(props) {
  return (
      <div className={styles.hidden}>
        <ColorModeToggle {...props} />
      </div>
  );
}
