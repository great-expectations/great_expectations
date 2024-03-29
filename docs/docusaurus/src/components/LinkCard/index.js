import React from 'react';
import Link from '@docusaurus/Link';
import VersionedLink from '@site/src/components/VersionedLink'
import styles from './styles.module.css';

/**
 * Version-safe link card component.
 * Wraps VersionedLink with card styling.
 */
export default function LinkCard({
  label,
  description,
  icon,
  to,
  truncate,
  topIcon
}) {
  return (
    <VersionedLink className={styles.linkCard} id="linkCard" to={to} data-truncate={truncate} data-top-icon={topIcon}>
      {icon && (
        <div className={styles.linkCard__icon}>
          <img src={icon} alt='' />
        </div>
      )}
      <div className={styles.linkCard__copy}>
        <p className={styles.linkCard__label}>{label}</p>
        {description && (
          <p className={styles.linkCard__description}>{description}</p>
        )}
      </div>
    </VersionedLink>
  )
}