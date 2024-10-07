import React, {useEffect, useState} from 'react';
import styles from './styles.module.scss';
import {useLocation} from "@docusaurus/router";
import useBaseUrl from "@docusaurus/useBaseUrl";
import { posthog as posthogJS } from 'posthog-js';
import useDocusaurusContext from "@docusaurus/useDocusaurusContext";
const CREATE_JIRA_TICKET_IN_DOCS_BOARD_ENDPOINT_URL = "/.netlify/functions/createJiraTicketInDocsBoard";

export default function WasThisHelpful(){
    const { pathname } = useLocation();
    const [feedbackSent, setFeedbackSent] = useState(false)
    const [isOpen, setIsOpen] = useState(false);
    const [error, setError] = useState(false);
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
        selectedValue: '',
        description: ''
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
        setError(false)
    }

    const sendReview = async (e) => {
        e.preventDefault()
        if (formData.description) {
            setError(false)
            posthog.capture("survey sent", {
                $survey_id: '018dd725-c595-0000-00c6-6eec1b197fd0',
                $survey_response: formData.name,
                $survey_response_1: formData.email,
                $survey_response_2: formData.description,
                $survey_response_3: pathname,
                $survey_response_4: formData.selectedValue.replaceAll('-', ' ')
            })
            try {
                const response = await fetch(CREATE_JIRA_TICKET_IN_DOCS_BOARD_ENDPOINT_URL, {
                    method: "POST",
                    headers: {
                        "Content-Type": "application/json",
                    },
                    body: JSON.stringify({...formData, pathname })
                });
                if (response.ok) {
                    setIsOpen(false)
                } else {
                    setError(true)
                }
            } catch (error) {
                setError(true)
            }

        }
    }

    const closeImg = useBaseUrl(`img/close_icon.svg`);

    return <>
        <hr className={styles.feedbackDivider}/>


        { !feedbackSent && <section className={styles.feedbackCard}>
            <h3 className={styles.feedbackCardTitle}>Was this topic helpful?</h3>
            <div className={styles.feedbackCardActions}>
                <button disabled={feedbackSent} className={styles.feedbackButton}
                        onClick={() => handleFeedbackReaction('docs_feedback.yes')}>Yes
                </button>
                <button disabled={feedbackSent} className={styles.feedbackButton}
                        onClick={handleNegativeFeedbackReaction}>No
                </button>
            </div>
        </section>}

        {isOpen && <>
            <div className={styles.overlay} onClick={dismissFeedbackModal}/>
            <dialog className={styles.modal}>
                <section className={styles.modalHeader}>
                    <h5 className={styles.modalHeaderTitle}>What is the problem?</h5>
                    <img src={closeImg} className={styles.modalHeaderCloseButton}
                         alt="Close icon" onClick={dismissFeedbackModal}/>
                </section>

                <form onSubmit={sendReview} className={styles.modalContent}>

                    <div className={styles.textInputs}>
                        <div className={styles.modalTextContainer}>
                            <label className={styles.modalTextLabel}>Name (optional)</label>
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
                            <label className={styles.modalTextLabel}>Email (optional)</label>
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
                        <label className={styles.radioOption}>
                            <input type="radio" name="selectedValue" value="language-typo"
                                   checked={formData.selectedValue === 'language-typo'} onChange={handleChange}/>
                            Language Typo
                        </label>
                    </div>

                    <div>
                        <label className={styles.radioOption}>
                            <input type="radio" name="selectedValue" value="inaccurate"
                                   checked={formData.selectedValue === 'inaccurate'} onChange={handleChange}/>
                            Inaccurate
                        </label>
                    </div>

                    <div>
                        <label className={styles.radioOption}>
                            <input type="radio" name="selectedValue" value="code-sample-errors"
                                   checked={formData.selectedValue === 'code-sample-errors'}
                                   onChange={handleChange}/>
                            Code sample errors
                        </label>
                    </div>

                    <div>
                        <label className={styles.radioOption}>
                            <input type="radio" name="selectedValue" value="need-gx-support"
                                   checked={formData.selectedValue === 'need-gx-support'}
                                   onChange={handleChange}/>
                            I need GX support
                        </label>
                    </div>

                    {formData.selectedValue === 'need-gx-support' &&
                        <p style={{marginBottom: '0'}}>
                            Visit our <a href="https://docs.greatexpectations.io/docs/resources/get_support">Get
                            Support
                            page</a>.
                        </p>}

                    {['language-typo', 'inaccurate', 'code-sample-errors'].includes(formData.selectedValue) &&
                        <textarea
                            name="description"
                            value={formData.description}
                            className={styles.modalTextInput + ' ' + styles.modalTextareaInput}
                            onChange={handleChange}
                            placeholder={formData.selectedValue === 'language-typo' ? "Describe the typo that you've found." : "Try to be specific and detailed."}
                        />
                    }

                    {error && <p className={styles.errorMessage}>An error occurred, please try again later.</p>}

                    <input type="submit" disabled={!formData.description} className={styles.submitButton}
                           value="Submit"/>
                </form>
            </dialog>
        </>}

        { feedbackSent && !isOpen &&
            <div className="alert alert--secondary" role="alert">
                Thank you for helping us improve our documentation!
            </div> }
    </>
}
