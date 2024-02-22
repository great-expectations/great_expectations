import React, {useState} from 'react';
import styles from './styles.module.css';
import {useLocation} from "@docusaurus/router";

export default function WasThisHelpful(){

    const { pathname } = useLocation();
    const [feedbackSent, setFeedbackSent] = useState(false)

    const handleFeedbackReaction = (eventName) => {
        if(!feedbackSent){
            setFeedbackSent(true)
            posthog.capture(eventName, { doc_url: pathname })
        }
    };

    return <>
            <hr className={styles.feedbackDivider}></hr>
            <div className={styles.feedbackCard}>
                <h3 className={styles.feedbackCardTitle}>Was this helpful?</h3>
                <div className={styles.feedbackCardActions}>
                    <button className={feedbackSent ? styles.inactiveFeedbackButton : styles.feedbackButton} onClick={() => handleFeedbackReaction('docs_feedback.yes')}>Yes</button>
                    <button className={feedbackSent ? styles.inactiveFeedbackButton : styles.feedbackButton} onClick={() => handleFeedbackReaction('docs_feedback.no')}>No</button>
                </div>
            </div>
        </>
}
