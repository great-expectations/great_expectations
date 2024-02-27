import React, {useState} from 'react';
import styles from './styles.module.scss';
import {useLocation} from "@docusaurus/router";
import useBaseUrl from "@docusaurus/useBaseUrl";

export default function WasThisHelpful(){

    const { pathname } = useLocation();
    const [feedbackSent, setFeedbackSent] = useState(false)
    const [isOpen, setIsOpen] = useState(false);

    const [formData, setFormData] = useState({
        name: '',
        email: '',
        description: '',
    });

    const handleChange = (e) => {
        const { name, value } = e.target
        setFormData((prevData) => ({
            ...prevData,
            [name]: value,
        }))
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

    const dismissFeedbackModal = () => {
        posthog.capture("survey dismissed", {
            $survey_id: '018dd725-c595-0000-00c6-6eec1b197fd0'
        })
        setIsOpen(false)
    }

    const sendReview = (e) => {
        e.preventDefault()
        if(formData.description){
            posthog.capture("survey sent", {
                $survey_id: '018dd725-c595-0000-00c6-6eec1b197fd0',
                $survey_response: formData.name,
                $survey_response_1: formData.email,
                $survey_response_2: formData.description,
                $survey_response_3: pathname
            })
            setIsOpen(false)
        }
    }

    const closeImg = useBaseUrl(`img/close_icon.svg`);

    return <>
            <hr className={styles.feedbackDivider}/>
            <section className={styles.feedbackCard}>
                <h3 className={styles.feedbackCardTitle}>Was this helpful?</h3>
                <div className={styles.feedbackCardActions}>
                    <button className={feedbackSent ? styles.inactiveFeedbackButton : styles.feedbackButton} onClick={() => handleFeedbackReaction('docs_feedback.yes')}>Yes</button>
                    <button className={feedbackSent ? styles.inactiveFeedbackButton : styles.feedbackButton} onClick={handleNegativeFeedbackReaction}>No</button>
                </div>
            </section>

            {isOpen && <>
                <div className={styles.overlay} onClick={dismissFeedbackModal}/>
                <div className={styles.modal}>
                    <div className={styles.modalHeader}>
                        <h5 className={styles.modalHeaderTitle}>Tell us more</h5>
                        <img src={closeImg} className={styles.modalHeaderCloseButton}
                             alt="Close icon" onClick={dismissFeedbackModal}/>
                    </div>

                    <form onSubmit={sendReview} className={styles.modalContent}>

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
                                <label className={styles.modalTextLabel}>Email</label>
                                <input
                                    type="email"
                                    name="email"
                                    className={styles.modalTextInput}
                                    value={formData.email}
                                    onChange={handleChange}
                                    placeholder="your_email@domain.com"
                                />
                            </div>
                        </div>
                        <div className={styles.modalTextContainer}>
                            <label className={styles.modalTextLabel}>Tell us more*</label>
                            <textarea
                                name="description"
                                value={formData.description}
                                className={styles.modalTextInput + ' ' + styles.modalTextareaInput}
                                onChange={handleChange}
                                required
                                placeholder="Provide as much detail as possible about the issue you
                                experienced or where improvement is needed. Detailed feedback helps
                                us better identify the problem and determine a solution."
                            />
                        </div>
                        <input type="submit" className={formData.description ? styles.submitButton : styles.inactiveSubmitButton} value="Submit"/>
                    </form>
                </div>
            </>}
    </>
}
