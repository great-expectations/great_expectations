import React from 'react';
import styles from './styles.module.css';
import useBaseUrl from "@docusaurus/useBaseUrl";
import {useLocation} from "@docusaurus/router";

export default function WasThisHelpful(){

    const thumbsUpImg = useBaseUrl(`img/thumbs_up_icon.svg`);
    const thumbsDownImg = useBaseUrl(`img/thumbs_down_icon.svg`);

    const { pathname } = useLocation();

    const handleThumbsUp = () => {
        posthog.capture('test_docs.thumbs_up', { doc_url: pathname })
    };

    const handleThumbsDown = () => {
        posthog.capture('test_docs.thumbs_down', { doc_url: pathname })
    };

    return <div className={styles.feedbackCard}>
            <h3 className={styles.feedbackCardTitle}>Was this helpful?</h3>
            <div className={styles.feedbackCardBody}>
                <img src={thumbsUpImg} className={styles.feedbackIcon}
                     alt="Thumbs up icon" onClick={handleThumbsUp}/>
                <img src={thumbsDownImg} className={styles.feedbackIcon}
                     alt="Thumbs down icon" onClick={handleThumbsDown}/>
            </div>
        </div>
}
