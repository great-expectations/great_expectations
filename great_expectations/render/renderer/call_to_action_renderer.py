class CallToActionRenderer(object):
    _document_defaults = {
        "cta_header": "What would you like to do next?",
        "styling": {
            "classes": [
                "border",
                "border-info",
                "alert",
                "alert-info",
                "fixed-bottom",
                "alert-dismissible",
                "fade",
                "show",
                "m-0",
                "rounded-0",
                "invisible"
            ],
            "attributes": {
                "id": "ge-cta-footer",
                "role": "alert"
            }
        }
    }
    
    @classmethod
    def render(cls, cta_object):
        """
        :param cta_object: dict
            {
                "cta_header": # optional, can be a string or string template
                "cta_buttons": # list of tuples of form (cta-button text, cta-button url)
            }
        :return: dict
            {
                "cta_header": # optional, can be a string or string template
                "cta_buttons": # list of tuples of form (cta-button text, cta-button url)
            }
        """
        
        if not cta_object.get("cta_header"):
            cta_object["cta_header"] = cls._document_defaults.get("cta_header")
        
        cta_object["styling"] = cls._document_defaults.get("styling")
        cta_object["tooltip_icon"] = {
            "template": "$icon",
            "params": {
                "icon": ""
            },
            "tooltip": {
                "content": "To disable this footer, set the show_cta_footer flag in your project config to false."
            },
            "styling": {
                "params": {
                    "icon": {
                        "tag": "i",
                        "classes": ["m-1", "fas", "fa-question-circle"],
                    }
                }
            }
        }
        
        return cta_object
