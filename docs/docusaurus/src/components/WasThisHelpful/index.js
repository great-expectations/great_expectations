import React, {useState} from 'react';
import styles from './styles.module.css';
import {useLocation} from "@docusaurus/router";
import useBaseUrl from "@docusaurus/useBaseUrl";

export default function WasThisHelpful(){

    const { pathname } = useLocation();
    const [feedbackSent, setFeedbackSent] = useState(false)
    const [isOpen, setIsOpen] = useState(false);

    const [formData, setFormData] = useState({
        name: '',
        workEmail: '',
        description: '',
    });

    const handleChange = (e) => {
        const { name, value } = e.target;
        setFormData((prevData) => ({
            ...prevData,
            [name]: value,
        }));
    };

    const handleFeedbackReaction = (eventName) => {
        if(!feedbackSent){
            setFeedbackSent(true)
            posthog.capture(eventName, { doc_url: pathname })
        }
    };

    const handleNegativeFeedbackReaction = () => {
        setIsOpen(true)
        handleFeedbackReaction('docs_feedback.no')
    }

    const sendReview = () => {
        posthog.capture('form_submitted', {
            form_data: formData,
        })
        console.log('AAAAAAAAAAAAAAAAAAAA',posthog.capture('form_submitted', {
            form_data: formData,
        }))
        setIsOpen(false)
    }

    const closeImg = useBaseUrl(`img/close_icon.svg`);

    return <>
            <hr className={styles.feedbackDivider}/>
            <div className={styles.feedbackCard}>
                <h3 className={styles.feedbackCardTitle}>Was this helpful?</h3>
                <section className={styles.feedbackCardActions}>
                    <button className={feedbackSent ? styles.inactiveFeedbackButton : styles.feedbackButton} onClick={() => handleFeedbackReaction('docs_feedback.yes')}>Yes</button>
                    <button className={feedbackSent ? styles.inactiveFeedbackButton : styles.feedbackButton} onClick={handleNegativeFeedbackReaction}>No</button>
                </section>
            </div>

            {isOpen && <>
                <div className={styles.overlay} onClick={() => setIsOpen(false)}/>
                <div className={styles.modal}>
                    <div className={styles.modalHeader}>
                        <h5 className={styles.modalHeaderTitle}>Tell Us More</h5>
                        <img src={closeImg} className={styles.modalHeaderCloseButton}
                             alt="Close icon" onClick={() => setIsOpen(false)}/>
                    </div>

                    <div className={styles.modalContent}>

                        Your opinion matters. Share your feedback here to help us improve
                        the quality of our documentation and ensure a better user experience.
                        Thank you for taking the time to share your experience.

                        <div className={styles.textInputs}>
                            <div className={styles.modalTextContainer}>
                                <label className={styles.modalTextLabel}>Name</label>
                                <input
                                    type="text"
                                    name="name"
                                    className={styles.modalTextInput}
                                    value={formData.name}
                                    onChange={handleChange}
                                    placeholder="Phillip"
                                />
                            </div>
                            <div className={styles.modalTextContainer}>
                                <label className={styles.modalTextLabel}>Work Email*</label>
                                <input
                                    type="email"
                                    name="workEmail"
                                    className={styles.modalTextInput}
                                    value={formData.workEmail}
                                    onChange={handleChange}
                                    placeholder="your_email@domain.com"
                                />
                            </div>
                        </div>
                        <div className={styles.modalTextContainer}>
                            <label className={styles.modalTextLabel}>Tell Us More</label>
                            <textarea
                                name="description"
                                value={formData.description}
                                className={styles.modalTextInput + ' ' + styles.modalTextareaInput}
                                onChange={handleChange}
                                placeholder="Provide as much detail as possible about the issue you
                                experienced or where improvement is needed. Detailed feedback helps
                                us better identify the problem and determine a solution."
                            />
                        </div>
                        <button className={styles.submitButton} onClick={sendReview}> Submit</button>
                    </div>
                </div>
            </>}
    </>
}
