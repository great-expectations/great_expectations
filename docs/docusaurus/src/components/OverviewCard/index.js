import React from 'react';
import styles from './styles.module.scss';

export default function OverviewCard(
    {
        title,
        children
    }
) {
    return (
        <div className={styles.overviewCard} id="overviewCard">
            <div className={styles.overviewCard__inner_container}>
                <h1>{title}</h1>
                <h4 className={styles.overviewCard__description}>{children}</h4>
            </div>
        </div>
    )
}