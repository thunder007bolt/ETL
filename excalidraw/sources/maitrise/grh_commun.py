"""
Ce qui est commun aux fiches de maîtrise du domaine de la gestion administrative (T58 à T65) :
le dossier de sortie, la date d'état, et les blocs tout faits (une règle, un constat, une erreur
des DTM, une question, un mot du vocabulaire, un lot, un chiffre, une question-réponse).

Le contenu des fiches est dans grh_fiches_01_03.py, grh_fiches_04_05_08.py,
grh_fiches_06_07.py et grh_fiches_09_11.py ; grh.py les rassemble dans FICHES.
"""

from fiches import Bloc

D = "4 gestion administrative/"
ETAT_AU = "état au 17/11/2026"


def regle(ref, titre, etat, regle_txt, pourquoi, preuve, question, reponse, pied):
    """Bloc d'une règle de calcul, toujours dans le même ordre."""
    return Bloc(titre, [("La règle", regle_txt), ("Pourquoi", pourquoi), ("La preuve", preuve),
                        (f"Si on te demande : {question}", reponse)],
                ref=ref, genre="regle", etat=etat, pied=pied)


def constat(ref, titre, fait, chiffre_txt, importance, pied):
    """Bloc d'un fait établi dans les données."""
    return Bloc(titre, [("Ce qu'on a découvert", fait), ("Le chiffre", chiffre_txt),
                        ("Pourquoi ça compte", importance)],
                ref=ref, genre="constat", etat="prouvé", pied=pied)


def erreur(ref, titre, verdict, fait, verifie, consequence, pied):
    """Bloc d'une erreur supposée des requêtes DTM actuelles."""
    return Bloc(titre, [("Ce que fait la requête DTM", fait), ("Ce qu'on a vérifié", verifie),
                        ("Ce que ça change", consequence)],
                ref=ref, genre="erreur", etat=verdict, pied=pied)


def question(ref, titre, q, pourquoi, change, proposition, pied=""):
    """Bloc d'une question encore ouverte."""
    return Bloc(titre, [("La question", q), ("Pourquoi on la pose", pourquoi),
                        ("Ce que change la réponse", change), ("Notre proposition", proposition)],
                ref=ref, genre="question", etat="ouvert", pied=pied)


def mot(titre, definition, exemple, attention=""):
    """Bloc d'un mot du vocabulaire."""
    champs = [("Ce que c'est", definition), ("Exemple", exemple)]
    if attention:
        champs.append(("Attention", attention))
    return Bloc(titre, champs, genre="notion")


def lot(ref, titre, question_txt, lance, trouve, change, pied):
    """Bloc d'une étape du travail (un lot de requêtes)."""
    return Bloc(titre, [("La question de départ", question_txt), ("Ce qu'on a lancé", lance),
                        ("Ce qu'on a trouvé", trouve), ("Ce que ça a changé", change)],
                ref=ref, genre="etape", etat="fait", pied=pied)


def chiffre(valeur, sens, source):
    """Bloc d'un chiffre à connaître par cœur."""
    return Bloc(valeur, [("", sens)], genre="constat", pied=source)


def qr(q, r, appui):
    """Bloc d'une question qu'on peut te poser, avec ta réponse."""
    return Bloc(q, [("Ta réponse", r), ("Ton appui", appui)], genre="reponse")
