const codeBlockTheme = {
    plain: {
        color: "#D8D8D8",
        backgroundColor: "#373F58"
    },
    styles: [{
        types: ["prolog"],
        style: {
            color: "rgb(0, 0, 128)"
        }
    }, {
        types: ["comment"],
        style: {
            color: "#B6C647"
        }
    }, {
        types: ["builtin", "changed", "keyword", "interpolation-punctuation", "boolean"],
        style: {
            color: "#F3C62D"
        }
    }, {
        types: ["number", "inserted"],
        style: {
            color: "#FD9BC1"
        }
    }, {
        types: ["constant"],
        style: {
            color: "rgb(100, 102, 149)"
        }
    }, {
        types: ["attr-name", "variable"],
        style: {
            color: "rgb(156, 220, 254)"
        }
    }, {
        types: ["deleted", "string", "attr-value", "template-punctuation"],
        style: {
            color: "#CE9178"
        }
    }, {
        types: ["selector"],
        style: {
            color: "rgb(215, 186, 125)"
        }
    }, {
        // Fix tag color
        types: ["tag"],
        style: {
            color: "rgb(78, 201, 176)"
        }
    }, {
        // Fix tag color for HTML
        types: ["tag"],
        languages: ["markup"],
        style: {
            color: "rgb(86, 156, 214)"
        }
    }, {
        types: ["punctuation", "operator"],
        style: {
            color: "#E1C2FA"
        }
    }, {
        // Fix punctuation color for HTML
        types: ["punctuation"],
        languages: ["markup"],
        style: {
            color: "#808080"
        }
    }, {
        types: ["function"],
        style: {
            color: "#E1C2FA"
        }
    }, {
        types: ["class-name"],
        style: {
            color: "rgb(78, 201, 176)"
        }
    }, {
        types: ["char"],
        style: {
            color: "rgb(209, 105, 105)"
        }
    }]
};

module.exports = codeBlockTheme
