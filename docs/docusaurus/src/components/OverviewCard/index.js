import React from 'react';
import styles from './styles.module.scss';

export default function OverviewCard(
    {
        title,
        children
    }
) {
    return (
        <header className={styles.overviewCard} id="overviewCard">
            <h1 className={styles.overviewCard__title}>{title}</h1>
            <h4 className={styles.overviewCard__description}>{children}</h4>
        </header>
    )
}
