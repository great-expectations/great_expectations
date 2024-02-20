import React, {useState} from 'react';
import styles from './styles.module.css';
import {useLocation} from "@docusaurus/router";

export default function WasThisHelpful(){

    const { pathname } = useLocation();
    const [buttonsClicked, setButtonsClicked] = useState(false)

    const handleThumbsUp = () => {
        setButtonsClicked(true)
        posthog.capture('test_docs.thumbs_up', { doc_url: pathname })
    };

    const handleThumbsDown = () => {
        setButtonsClicked(true)
        posthog.capture('test_docs.thumbs_down', { doc_url: pathname })
    };

    return <div className={styles.feedbackCard}>
            <h3 className={styles.feedbackCardTitle}>Was this helpful?</h3>
            <div className={styles.feedbackCardActions}>
                <button className={buttonsClicked ? styles.inactiveFeedbackButton : styles.feedbackButton} onClick={handleThumbsUp}>Yes</button>
                <button className={buttonsClicked ? styles.inactiveFeedbackButton : styles.feedbackButton} onClick={handleThumbsDown}>No</button>
            </div>
        </div>
}
