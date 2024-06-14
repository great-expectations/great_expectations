import React, {useEffect, useState} from 'react';
import styles from './styles.module.scss';
import {useLocation} from "@docusaurus/router";
import useBaseUrl from "@docusaurus/useBaseUrl";
import { posthog as posthogJS } from 'posthog-js';
import useDocusaurusContext from "@docusaurus/useDocusaurusContext";

export default function WasThisHelpful(){
    const { pathname } = useLocation();
    const [feedbackSent, setFeedbackSent] = useState(false)
    const [isOpen, setIsOpen] = useState(false);
    const config = useDocusaurusContext()

    useEffect(() => {
        if (window && !window.posthog) {
            // Checking if Posthog is already initialized
            posthogJS.init(config.siteConfig.customFields.posthogApiKey)
            window.posthog = posthogJS
        }
    }, []);

    const [formData, setFormData] = useState({
        name: '',
        email: '',
        languageTypo: '',
        inaccurate: '',
        codeSampleErrors: ''
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
                <h3 className={styles.feedbackCardTitle}>Was this topic helpful?</h3>
                <div className={styles.feedbackCardActions}>
                    <button disabled={feedbackSent} className={styles.feedbackButton} onClick={() => handleFeedbackReaction('docs_feedback.yes')}>Yes</button>
                    <button disabled={feedbackSent} className={styles.feedbackButton} onClick={handleNegativeFeedbackReaction}>No</button>
                </div>
            </section>

            {!isOpen && <>
                <div className={styles.overlay} onClick={dismissFeedbackModal}/>
                <dialog className={styles.modal}>
                    <section className={styles.modalHeader}>
                        <h5 className={styles.modalHeaderTitle}>What is the problem?</h5>
                        <img src={closeImg} className={styles.modalHeaderCloseButton}
                             alt="Close icon" onClick={dismissFeedbackModal}/>
                    </section>

                    <form onSubmit={sendReview} className={styles.modalContent}>

                        <div>If you’re not reporting documentation issues such as typos, missing content, or code
                            inaccuracies, post your
                            comments or feedback on <a href="https://discourse.greatexpectations.io/">Discourse</a>.
                            Thank you for helping us improve our documentation.
                        </div>

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

                        <div>
                            <input type="radio" id="language-typo" name="feedback" value="language-typo"/>
                            <label htmlFor="language-typo" className={styles.modalTextLabel}>Language Typo</label>
                            <textarea
                                name="description"
                                value={formData.languageTypo}
                                className={styles.modalTextInput + ' ' + styles.modalTextareaInput + ' ' + styles.hiddenRadioButtonAdditionalInfo}
                                onChange={handleChange}
                                required
                                placeholder="Describe the typo that you've found."
                            />
                        </div>

                        <div>
                            <input type="radio" id="inaccurate" name="feedback" value="inaccurate"/>
                            <label htmlFor="inaccurate" className={styles.modalTextLabel}>Inaccurate</label>
                            <textarea
                                name="inaccurate"
                                value={formData.inaccurate}
                                className={styles.modalTextInput + ' ' + styles.modalTextareaInput + ' ' + styles.hiddenRadioButtonAdditionalInfo}
                                onChange={handleChange}
                                required
                                placeholder="Try to be specific and detailed."
                            />
                        </div>

                        <div>
                            <input type="radio" id="code-sample-errors" name="feedback" value="code-sample-errors"/>
                            <label htmlFor="code-sample-errors" className={styles.modalTextLabel}>Code sample
                                errors</label>
                            <textarea
                                name="code-sample-errors"
                                value={formData.codeSampleErrors}
                                className={styles.modalTextInput + ' ' + styles.modalTextareaInput + ' ' + styles.hiddenRadioButtonAdditionalInfo}
                                onChange={handleChange}
                                required
                                placeholder="Try to be specific and detailed."
                            />
                        </div>

                        <div>
                            <input type="radio" id="need-gx-support" name="feedback" value="need-gx-support"/>
                            <label htmlFor="need-gx-support" className={styles.modalTextLabel}>I need GX support</label>
                            <p className={styles.hiddenRadioButtonAdditionalInfo}>
                                Visit our <a href="https://docs.greatexpectations.io/docs/resources/get_support">Get Support
                                page</a>.
                            </p>
                        </div>

                        <input type="submit" disabled={!formData.description} className={styles.submitButton}
                               value="Submit"/>
                    </form>
                </dialog>
            </>}
    </>
}
