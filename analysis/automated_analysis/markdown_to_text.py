#Markdown compiling
import mistune
import mistune.renderers

#NLP
import nltk
import nltk.tokenize


valid_end_punct = set((".","!","?",'"',"'"))
def finish_incomplete_sentences(text,flag_components=False):
    sentences = nltk.tokenize.sent_tokenize(text)
    if len(sentences) == 0:
        return ""
    words = nltk.tokenize.word_tokenize(sentences[-1])
    if not words[-1] in valid_end_punct:
        if flag_components:
            sentences[-1] = sentences[-1] + "_inserted_ . _inserted_"
        else:
            #Close sentence
            sentences[-1] = sentences[-1] + "."
    return " ".join(sentences)

class StraightTextRenderer(mistune.renderers.BaseRenderer):
    
    def __init__(self,flag_components):
        self.flag_components = flag_components
    
    def text(self, text):
        return text

    def link(self, link, text=None, title=None):
        if text is None:
            return "link"
        else:
            return text

    def image(self, src, alt="", title=None):
        return ""

    def emphasis(self, text):
        return text

    def strong(self, text):
        return text

    def codespan(self, text):
        if self.flag_components:
            return "\n_codespan_%s_codespan\n" % text
        else:
            return "\n"

    def linebreak(self):
        if self.flag_components:
            return "\n_line break_\n"
        else:
            return "\n"

    def inline_html(self, html):
        if self.flag_components:
            return '\n_inline-html_%s_inline-html_\n' % html
        else:
            #HTML isn't prose
            return "\n"

    def paragraph(self, text):
        if text == '': return text
        paragraphs = text.split('\n')
        paragraphs = (finish_incomplete_sentences(para,flag_components=self.flag_components) for para in paragraphs)
        text = "\n".join(paragraphs)
        if self.flag_components:
            return "\n_paragraph_\n" + text + "\n_paragraph_\n"
        else:
            return text + "\n"

    def heading(self, text, level):
        if self.flag_components:
            return '\n_heading %d_ %s\n' % (level,text)
        else:
            #Headings aren't prose
            return "\n"

    def newline(self):
        if self.flag_components:
            return '\n_newline_\n'
        else:
            return "\n"

    def thematic_break(self):
        if self.flag_components:
            return '\n_thematic-break_\n'
        else:
            return "\n"

    def block_text(self, text):
        if self.flag_components:
            return '\n_block-text_%s_block-text_\n' % text
        else:
            return "%s\n" % text

    def block_code(self, code, info=None):
        if self.flag_components:
            if not code.strip():
                return "\n"
            else:
                return '\n_block-code_%s_block-code_\n' % code
        else:
            #This stuff usually isn't code, treat it as a paragraph
            return self.paragraph(code)

    def block_quote(self, text):
        if self.flag_components:
            return '\n_block-quote_%s_block-quote_\n' % text
        else:
            return "%s\n" % text

    def block_html(self, html):
        if self.flag_components:
            return "\n_block-html_%s_block-html\n" % html
        else:
            #HTML isn't prose
            return  "\n"

    def block_error(self, html):
        if self.flag_components:
            return "\n_block-error_%s_block-error\n" % html
        else:
            #Errors aren't prose
            return "\n"

    def list(self, text, ordered, level, start=None):
        if text == '': return text
        items = text.split('\n')
        items = [finish_incomplete_sentences(item,flag_components=self.flag_components) for item in items]
        text = " ".join(items)
        if self.flag_components:
            return "\n_list %s %d_\n%s\n_list_\n" % (ordered, level, text)
        else:
            return text + "\n"

    def list_item(self, text, level):
        return "%s\n" % text
    
    def strikethrough(self, text):
        return ""
    
    def table(self, text):
        if self.flag_components:
            return '\n_table_%s_table_\n' % (text)
        else:
            return "\n"
    
    def table_cell(self, content, align=None, is_head=False):
        if self.flag_components:
            return '\n_cell_\n'
        else:
            return f"{content} "
    
    def table_head(self, content):
        if self.flag_components:
            return '\n_head_\n'
        else:
            return ""
        
    def table_row(self, content):
        if self.flag_components:
            return '_row_%s_row_\n' % content
        else:
            return f"{content}.\n"
        
    def table_body(self, content):
        if self.flag_components:
            return '_body_%s_body_\n' % content
        else:
            return content
    
markdown = mistune.create_markdown(renderer=StraightTextRenderer(False))
markdown_debug = mistune.create_markdown(renderer=StraightTextRenderer(True))

#Install mistune plugins
import mistune.plugins
mistune.plugins.plugin_table(markdown)
mistune.plugins.plugin_strikethrough(markdown)
mistune.plugins.plugin_table(markdown_debug)
mistune.plugins.plugin_strikethrough(markdown_debug)

def clean(policy_text):
    return markdown(policy_text)

def clean_debug(policy_text):
    return markdown_debug(policy_text)
