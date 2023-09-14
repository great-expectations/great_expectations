import React from 'react';
import styles from './styles.module.css';
import cn from 'clsx';
import { useThemeConfig } from '@docusaurus/theme-common';

function useGXCardConfig() {
  // TODO temporary casting until ThemeConfig type is improved
  return useThemeConfig().GXCard;
}

export default function GXCard(
  {
    title,
    description
  }
) {
  const content = useGXCardConfig();
  return (
    <div className={styles.GXCard} id="GXCard">
      <h2 className={styles.GXCard__title}>{title || content.title}</h2>
      <p className={styles.GXCard__description}>{description || content.description}</p>
      <div className={styles.GXCard__buttons}>
        <a className={cn(styles.button)} href={content.buttons.primary.href}>{content.buttons.primary.label}</a>
        <a className={cn(styles.button, styles.button__outline)} href={content.buttons.secondary.href}>{content.buttons.secondary.label}</a>
      </div>
    </div>
  )
}