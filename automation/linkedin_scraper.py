import re
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ContentAnalyzer:
    """NLP-based content analyzer to identify knowledge-sharing and educational posts"""
    
    def __init__(self):
        # Knowledge/Tips indicators
        self.knowledge_keywords = {
            'instructional': ['how to', 'guide', 'tutorial', 'step by step', 'walkthrough', 'learn', 
                            'understand', 'master', 'beginner', 'advanced', 'course', 'lesson'],
            
            'tips': ['tip', 'hack', 'trick', 'secret', 'best practice', 'pro tip', 'advice', 
                    'recommendation', 'suggestion', 'insight', 'strategy', 'technique'],
            
            'educational': ['explain', 'breakdown', 'analysis', 'deep dive', 'overview', 'summary',
                          'framework', 'methodology', 'approach', 'concept', 'principle', 'theory'],
            
            'actionable': ['implement', 'apply', 'use', 'try', 'start', 'begin', 'practice',
                         'follow', 'adopt', 'execute', 'action', 'do this', 'next step'],
            
            'sharing': ['share', 'learned', 'discovered', 'found out', 'realized', 'experience',
                       'knowledge', 'wisdom', 'lessons', 'takeaway', 'key points']
        }
        
        # Sequential/instructional patterns
        self.instructional_patterns = [
            r'\b(?:first|1st|step\s+1)\b',
            r'\b(?:second|2nd|step\s+2|then|next)\b', 
            r'\b(?:third|3rd|step\s+3|finally|last)\b',
            r'\b(?:here\'s\s+how|here\s+are|follow\s+these)\b',
            r'\b\d+[\.\)]\s+\w+',  # Numbered lists
            r'\b(?:step\s+\d+|point\s+\d+)\b'
        ]
        
        # Question patterns that indicate knowledge sharing
        self.question_patterns = [
            r'\bwhat\s+is\b', r'\bhow\s+to\b', r'\bwhy\s+does\b', r'\bwhen\s+should\b',
            r'\bwhich\s+\w+\b', r'\bwhere\s+can\b', r'\bhow\s+do\s+you\b'
        ]
        
        # Technical/professional domains
        self.domain_keywords = {
            'tech': ['api', 'algorithm', 'code', 'programming', 'software', 'development', 'ai', 'ml',
                    'data', 'database', 'cloud', 'devops', 'javascript', 'python', 'react'],
            
            'business': ['strategy', 'marketing', 'sales', 'leadership', 'management', 'productivity',
                        'entrepreneurship', 'startup', 'growth', 'revenue', 'roi', 'kpi'],
            
            'career': ['interview', 'resume', 'networking', 'skills', 'career', 'job search',
                      'professional development', 'certification', 'training'],
            
            'design': ['ux', 'ui', 'design thinking', 'user experience', 'prototyping', 'wireframe',
                      'usability', 'interface', 'visual design']
        }
        
        # Non-knowledge content patterns (to filter out)
        self.non_knowledge_patterns = [
            # Personal announcements
            r'\b(?:excited\s+to\s+announce|happy\s+to\s+share|pleased\s+to\s+share)\b',
            r'\b(?:thrilled|honored|grateful|blessed)\b',
            
            # Job postings
            r'\b(?:we\'re\s+hiring|join\s+our\s+team|looking\s+for|open\s+position)\b',
            r'\b(?:apply\s+now|send\s+your\s+resume|dm\s+me)\b',
            
            # Event promotions
            r'\b(?:register\s+now|save\s+the\s+date|upcoming\s+event|webinar)\b',
            
            # Simple congratulations
            r'\b(?:congratulations|congrats|well\s+done|amazing\s+work)\b',
            
            # Company promotions
            r'\b(?:proud\s+of\s+our\s+team|check\s+out\s+our|visit\s+our\s+website)\b'
        ]

        # Enhanced promotional patterns for removal (updated with more comprehensive patterns)
        self.promotional_patterns = [
            # Follow patterns - more comprehensive
            r'(?:don\'t\s+forget\s+to\s+follow|follow\s+me\s+for\s+more|follow\s+@?\w+(?:\s+\w+)*\s+for\s+more).*?(?:updates?|content|tips|insights?|posts?|information)\.?',
            r'follow\s+@?\w+(?:\s+\w+)*\s+for\s+more.*?(?:updates?|content|tips|insights?|posts?|such\s+updates?)\.?',
            r'make\s+sure\s+to\s+follow\s+.*?for\s+more.*?(?:updates?|content|tips|insights?)\.?',
            r'follow\s+@?\w+(?:\s+\w+)*\s+to\s+stay\s+updated.*?\.?',
            r'follow\s+@?\w+(?:\s+\w+)*\s+if\s+you\s+want.*?\.?',
            
            # Subscribe patterns  
            r'(?:subscribe|hit\s+the\s+bell|turn\s+on\s+notifications?).*?(?:updates?|content|videos?)\.?',
            r'don\'t\s+forget\s+to\s+subscribe.*?\.?',
            
            # Like and share patterns
            r'(?:like\s+and\s+share|share\s+if\s+you\s+found|hit\s+like\s+if).*?\.?',
            r'(?:smash\s+that\s+like\s+button|give\s+this\s+a\s+like).*?\.?',
            r'if\s+you\s+found\s+this\s+helpful.*?(?:like|share).*?\.?',
            
            # Comment CTAs
            r'(?:comment\s+below|let\s+me\s+know\s+in\s+the\s+comments|drop\s+a\s+comment).*?\.?',
            r'what\s+are\s+your\s+thoughts\?\s*comment\s+below\.?',
            r'share\s+your\s+thoughts\s+in\s+the\s+comments.*?\.?',
            
            # Generic CTAs
            r'(?:connect\s+with\s+me|dm\s+me|reach\s+out\s+to\s+me).*?\.?',
            r'tag\s+someone\s+who.*?\.?',
            r'share\s+this\s+with.*?\.?',
            r'send\s+this\s+to\s+someone.*?\.?',
            
            # Connection requests
            r'connect\s+with\s+me\s+on\s+linkedin.*?\.?',
            r'add\s+me\s+on\s+linkedin.*?\.?',
            
            # Newsletter/email signups
            r'subscribe\s+to\s+my\s+newsletter.*?\.?',
            r'join\s+my\s+mailing\s+list.*?\.?',
            r'sign\s+up\s+for.*?newsletter.*?\.?',
            
            # Self-promotion
            r'check\s+out\s+my\s+(?:website|blog|course|book).*?\.?',
            r'visit\s+my\s+(?:website|profile|page).*?\.?',
            r'link\s+in\s+(?:bio|comments?).*?\.?',
            
            # Hashtag spam (excessive hashtags at end)
            r'(?:#\w+\s*){5,}'
,
            
            # Keep updated patterns
            r'stay\s+tuned\s+for\s+more.*?\.?',
            r'more\s+(?:content|tips|updates?)\s+coming\s+soon.*?\.?',
        ]
    
    def clean_promotional_content(self, content: str) -> dict:
        """Remove promotional CTAs and social media engagement bait from content"""
        original_content = content
        cleaned_content = content
        removed_parts = []
        
        # Remove promotional patterns
        for pattern in self.promotional_patterns:
            matches = re.finditer(pattern, cleaned_content, re.IGNORECASE | re.MULTILINE)
            for match in matches:
                matched_text = match.group(0).strip()
                if matched_text:
                    removed_parts.append(matched_text)
                    cleaned_content = cleaned_content.replace(matched_text, '').strip()
        
        # Clean up extra whitespace and punctuation left after removals
        cleaned_content = re.sub(r'\s+', ' ', cleaned_content)  # Multiple spaces to single
        cleaned_content = re.sub(r'\.{2,}', '.', cleaned_content)  # Multiple dots
        cleaned_content = re.sub(r',\s*,', ',', cleaned_content)  # Double commas
        cleaned_content = re.sub(r'\s+([,.!?])', r'\1', cleaned_content)  # Space before punctuation
        
        # Remove trailing incomplete sentences that might be cut off CTAs
        sentences = cleaned_content.split('.')
        if len(sentences) > 1:
            last_sentence = sentences[-1].strip()
            # If last sentence is very short and contains certain words, remove it
            cta_indicators = ['follow', 'subscribe', 'like', 'share', 'comment', 'connect', 'dm', 'tag']
            if (len(last_sentence) < 50 and 
                any(indicator in last_sentence.lower() for indicator in cta_indicators)):
                sentences = sentences[:-1]
                cleaned_content = '.'.join(sentences) + '.'
        
        # Final cleanup
        cleaned_content = cleaned_content.strip()
        if cleaned_content.endswith('..'):
            cleaned_content = cleaned_content[:-1]
        
        was_cleaned = len(removed_parts) > 0 or len(cleaned_content) < len(original_content) * 0.9
        characters_removed = len(original_content) - len(cleaned_content)
        
        return {
            'cleaned_content': cleaned_content,
            'was_cleaned': was_cleaned,
            'removed_promotional_parts': removed_parts,
            'characters_removed': characters_removed,
            'original_length': len(original_content),
            'cleaned_length': len(cleaned_content)
        }
    
    def analyze_content(self, content: str) -> dict:
        """Comprehensive content analysis to determine if post shares knowledge/tips"""
        
        if not content or len(content.strip()) < 50:
            return {
                'is_knowledge_post': False,
                'confidence': 0.0,
                'reasoning': 'Content too short or empty',
                'category': 'insufficient_content',
                'final_content': content,
                'cleaning_result': {}
            }
        
        # First, clean promotional content
        cleaning_result = self.clean_promotional_content(content)
        cleaned_content = cleaning_result['cleaned_content']
        
        # Check if we still have enough content after cleaning
        if len(cleaned_content.strip()) < 30:
            return {
                'is_knowledge_post': False,
                'confidence': 0.0,
                'reasoning': 'Insufficient content after removing promotional elements',
                'category': 'mostly_promotional',
                'final_content': cleaned_content,
                'cleaning_result': cleaning_result
            }
        
        # Normalize content for analysis
        normalized_content = self._normalize_text(cleaned_content)
        
        # Calculate various scores
        scores = {
            'knowledge_score': self._calculate_knowledge_score(normalized_content),
            'instructional_score': self._calculate_instructional_score(normalized_content),
            'technical_score': self._calculate_technical_score(normalized_content),
            'actionable_score': self._calculate_actionable_score(normalized_content),
            'educational_structure_score': self._calculate_structure_score(normalized_content),
            'non_knowledge_penalty': self._calculate_non_knowledge_penalty(normalized_content)
        }
        
        # Calculate overall confidence
        base_score = (
            scores['knowledge_score'] * 0.25 +
            scores['instructional_score'] * 0.20 +
            scores['technical_score'] * 0.15 +
            scores['actionable_score'] * 0.20 +
            scores['educational_structure_score'] * 0.20
        )
        
        # Apply penalty for non-knowledge content
        final_score = max(0, base_score - scores['non_knowledge_penalty'])
        
        # Determine category and threshold
        is_knowledge = final_score >= 0.4  # Threshold for knowledge content
        category = self._determine_category(scores, cleaned_content)
        
        return {
            'is_knowledge_post': is_knowledge,
            'confidence': round(final_score, 3),
            'reasoning': self._generate_reasoning(scores, is_knowledge),
            'category': category,
            'detailed_scores': scores,
            'content_length': len(cleaned_content),
            'word_count': len(cleaned_content.split()),
            'final_content': cleaned_content,
            'cleaning_result': cleaning_result
        }
    
    def _normalize_text(self, text: str) -> str:
        """Normalize text for analysis"""
        # Convert to lowercase
        text = text.lower()
        # Remove extra whitespace
        text = ' '.join(text.split())
        # Remove some punctuation but keep sentence structure
        text = text.replace('\n', ' ').replace('\t', ' ')
        return text
    
    def _calculate_knowledge_score(self, content: str) -> float:
        """Calculate score based on knowledge-sharing keywords"""
        total_score = 0
        word_count = len(content.split())
        
        if word_count == 0:
            return 0
        
        for category, keywords in self.knowledge_keywords.items():
            category_score = 0
            for keyword in keywords:
                if keyword in content:
                    # Weight longer, more specific phrases higher
                    weight = len(keyword.split()) * 0.5 + 1
                    category_score += weight
            
            # Normalize by category
            total_score += min(category_score / len(keywords), 1.0)
        
        return min(total_score / len(self.knowledge_keywords), 1.0)
    
    def _calculate_instructional_score(self, content: str) -> float:
        """Calculate score based on instructional patterns"""
        pattern_matches = 0
        
        for pattern in self.instructional_patterns:
            matches = len(re.findall(pattern, content, re.IGNORECASE))
            pattern_matches += matches
        
        # Also check for question patterns
        question_matches = 0
        for pattern in self.question_patterns:
            if re.search(pattern, content, re.IGNORECASE):
                question_matches += 1
        
        total_matches = pattern_matches + question_matches
        
        # Normalize based on content length
        word_count = len(content.split())
        if word_count < 20:
            return 0
        
        return min(total_matches / (word_count / 50), 1.0)
    
    def _calculate_technical_score(self, content: str) -> float:
        """Calculate score based on technical/professional domain keywords"""
        total_score = 0
        
        for domain, keywords in self.domain_keywords.items():
            domain_score = 0
            for keyword in keywords:
                if keyword in content:
                    domain_score += 1
            
            if domain_score > 0:
                total_score += min(domain_score / len(keywords), 0.5)
        
        return min(total_score, 1.0)
    
    def _calculate_actionable_score(self, content: str) -> float:
        """Calculate score based on actionable advice indicators"""
        actionable_indicators = [
            r'\byou\s+(?:should|can|need|must|have\s+to)\b',
            r'\bif\s+you\b',
            r'\btry\s+(?:this|these|to)\b',
            r'\bstart\s+(?:by|with|doing)\b',
            r'\bmake\s+sure\b',
            r'\bremember\s+to\b',
            r'\bdon\'t\s+forget\b',
            r'\bpro\s+tip\b',
            r'\bkey\s+takeaway\b'
        ]
        
        matches = 0
        for pattern in actionable_indicators:
            matches += len(re.findall(pattern, content, re.IGNORECASE))
        
        # Check for imperative sentences (commands/instructions)
        sentences = re.split(r'[.!?]+', content)
        imperative_count = 0
        
        for sentence in sentences:
            sentence = sentence.strip()
            if sentence and len(sentence.split()) > 3:
                # Simple heuristic for imperative sentences
                if re.match(r'^(?:start|try|use|apply|implement|follow|avoid|remember|make|do|don\'t)', sentence.strip()):
                    imperative_count += 1
        
        total_score = matches + (imperative_count * 0.5)
        word_count = len(content.split())
        
        return min(total_score / (word_count / 30), 1.0)
    
    def _calculate_structure_score(self, content: str) -> float:
        """Calculate score based on educational content structure"""
        structure_score = 0
        
        # Check for numbered or bulleted lists
        if re.search(r'^\d+[\.\)]\s+', content, re.MULTILINE):
            structure_score += 0.3
        
        # Check for clear sections/headers
        if re.search(r'^(?:[A-Z][^a-z]*|#|\*\*)', content, re.MULTILINE):
            structure_score += 0.2
        
        # Check for explanation patterns
        explanation_patterns = [
            r'\bfor\s+example\b',
            r'\bsuch\s+as\b',
            r'\bthis\s+means\b',
            r'\bin\s+other\s+words\b',
            r'\bto\s+clarify\b',
            r'\bspecifically\b'
        ]
        
        for pattern in explanation_patterns:
            if re.search(pattern, content, re.IGNORECASE):
                structure_score += 0.1
        
        # Check for conclusion/summary patterns
        conclusion_patterns = [
            r'\bin\s+conclusion\b',
            r'\bto\s+summarize\b',
            r'\bkey\s+takeaways?\b',
            r'\bbottom\s+line\b',
            r'\bto\s+wrap\s+up\b'
        ]
        
        for pattern in conclusion_patterns:
            if re.search(pattern, content, re.IGNORECASE):
                structure_score += 0.15
        
        return min(structure_score, 1.0)
    
    def _calculate_non_knowledge_penalty(self, content: str) -> float:
        """Calculate penalty for non-knowledge content patterns"""
        penalty = 0
        
        for pattern in self.non_knowledge_patterns:
            if re.search(pattern, content, re.IGNORECASE):
                penalty += 0.2
        
        # Additional penalties
        # Too much self-promotion
        self_promo_words = ['my company', 'our product', 'our service', 'buy now', 'purchase']
        promo_count = sum(1 for word in self_promo_words if word in content)
        if promo_count > 2:
            penalty += 0.3
        
        # Too many emojis (often indicates personal/promotional content)
        emoji_count = len(re.findall(r'[😀-🙏🌀-🗿🚀-🛿☀-⛿✀-➿]', content))
        if emoji_count > 5:
            penalty += 0.2
        
        return min(penalty, 1.0)
    
    def _determine_category(self, scores: dict, content: str) -> str:
        """Determine the category of the post"""
        if scores['instructional_score'] > 0.5:
            return 'tutorial_guide'
        elif scores['actionable_score'] > 0.5:
            return 'tips_advice'
        elif scores['technical_score'] > 0.4:
            return 'technical_knowledge'
        elif scores['educational_structure_score'] > 0.4:
            return 'educational_content'
        elif scores['knowledge_score'] > 0.3:
            return 'knowledge_sharing'
        elif scores['non_knowledge_penalty'] > 0.3:
            return 'promotional_personal'
        else:
            return 'general_content'
    
    def _generate_reasoning(self, scores: dict, is_knowledge: bool) -> str:
        """Generate human-readable reasoning for the classification"""
        if not is_knowledge:
            if scores['non_knowledge_penalty'] > 0.3:
                return "Filtered out: Contains promotional or personal announcement patterns"
            else:
                return "Low knowledge indicators: Lacks educational keywords, structure, or actionable advice"
        
        reasons = []
        if scores['knowledge_score'] > 0.3:
            reasons.append("Contains knowledge-sharing keywords")
        if scores['instructional_score'] > 0.3:
            reasons.append("Has instructional patterns")
        if scores['actionable_score'] > 0.3:
            reasons.append("Provides actionable advice")
        if scores['technical_score'] > 0.3:
            reasons.append("Contains domain expertise")
        if scores['educational_structure_score'] > 0.3:
            reasons.append("Well-structured educational content")
        
        return "Knowledge post: " + ", ".join(reasons)
