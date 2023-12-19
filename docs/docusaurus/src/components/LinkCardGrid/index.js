import React from 'react';
import styles from './styles.module.css';

export default function LinkCardGrid({ children }) {
  return (
    <div className={styles.linkCardGrid}>
      {children}
    </div>
  )
}