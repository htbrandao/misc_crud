import cv2
import pytesseract

import numpy as np


class Tesseract:

    @staticmethod
    def bytes_para_imagem(imagem: bytes, flag_cv2=cv2.COLOR_BGR2GRAY):
        """
        Converte bytes para imagem.
        Equivalente a

        ```
        f = 'path/to/file.jpeg'  # ou .png, etc
        cv2.imread(f)
        ```

        Exemplos de flag_cv2:
            - cv2.IMREAD_GRAYSCALE  -> 1 canal
            - cv2.COLOR_BGR2GRAY    -> 3 canais

        :param bytes imagem: bytes da imagem
        :param attr flag_cv2: cv2.IMREAD_GRAYSCALE
        :returns: array numpy dos pixels da imagem
        :rtype: numpy.ndarray
        """
        imagem = np.frombuffer(buffer=imagem, dtype=np.uint8)
        imagem = cv2.imdecode(imagem, flag_cv2)
        return imagem

    @staticmethod
    def escala_de_cinza(imagem):
        """
        Converte a imagem para escala de cinza (1 canal).

        :param np.array imagem: imagem em formato de array numpy
        :returns: imagem com filtro aplicado
        :rtype: np.array
        """
        return cv2.cvtColor(imagem, cv2.COLOR_BGR2GRAY)

    @staticmethod
    def remove_ruido(imagem):
        """
        Aplica o filtro median blur.

        :param np.array imagem: imagem em formato de array numpy
        :returns: imagem com filtro aplicado
        :rtype: np.array
        """
        return cv2.medianBlur(imagem, 5)

    @staticmethod
    def limiar(imagem):
        """
        Aplica o filtro de limiar.

        :param np.array imagem: imagem em formato de array numpy
        :returns: imagem com filtro aplicado
        :rtype: np.array
        """
        return cv2.threshold(imagem, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)[1]

    @staticmethod
    def dilatacao(imagem, kernel=np.ones((5, 5), np.uint8)):
        """
        Aplica o filtro de dilatação.

        :param np.array imagem: imagem em formato de array numpy
        :returns: imagem com filtro aplicado
        :rtype: np.array
        """
        return cv2.dilate(imagem, kernel, iterations=1)

    @staticmethod
    def erosao(imagem, kernel=np.ones((5, 5), np.uint8)):
        """
        Aplica o filtro de erosão.

        :param np.array imagem: imagem em formato de array numpy
        :returns: imagem com filtro aplicado
        :rtype: np.array
        """
        return cv2.erode(imagem, kernel, iterations=1)

    @staticmethod
    def abertura(imagem, kernel=np.ones((5, 5), np.uint8)):
        """
        Aplica o filtro de rosão seguida de dilatação.

        :param np.array imagem: imagem em formato de array numpy
        :returns: imagem com filtro aplicado
        :rtype: np.array
        """
        return cv2.morphologyEx(imagem, cv2.MORPH_OPEN, kernel)

    @staticmethod
    def deteccao_de_bordas(imagem):
        """
        Aplica o filtro de detecção de bordas de Canny

        :param np.array imagem: imagem em formato de array numpy
        :returns: imagem com filtro aplicado
        :rtype: np.array
        """
        return cv2.Canny(imagem, 100, 200)

    @staticmethod
    def correcao_de_desvio(imagem):
        """
        Realiza a correção de desvio (skew) na imagem.

        :param np.array imagem: imagem em formato de array numpy
        :returns: imagem com filtro aplicado
        :rtype: np.array
        """
        coordenadas = np.column_stack(np.where(imagem > 0))
        angulo = cv2.minAreaRect(coordenadas)[-1]
        if angulo < -45:
            angulo = -(90 + angulo)
        else:
            angulo = -angulo
        (h, w) = imagem.shape[:2]
        centro = (w // 2, h // 2)
        M = cv2.getRotationMatrix2D(centro, angulo, 1.0)
        rotacionado = cv2.warpAffine(imagem, M, (w, h), flags=cv2.INTER_CUBIC, borderMode=cv2.BORDER_REPLICATE)
        return rotacionado

    @staticmethod
    def ocr(imagem, lang: str = 'por', oem: int = 1, psm: int = 4) -> str:
        """
        Realiza o OCR na imagem.

        Para mais informações sobre a esecução do Tesseract:

        ```bash
        $ tesseract --version  # informa a versão do tesseract
        $ tesseract --help-oem  # lista as engines disponíveis
        $ tesseract --help-psm  # lista as formas de page segmentation
        $ tesseract --list-langs  # lista os idiomas disponíveis
        ```

        Sobre OEM:
            - 0: Legacy engine only.
            - 1: Neural nets LSTM engine only.
            - 2: Legacy + LSTM engines.
            - 3: Default, based on what is available.

        Sobre PSM:
            - 0: Orientation and script detection (OSD) only.
            - 1: Automatic page segmentation with OSD.
            - 2: Automatic page segmentation, but no OSD, or OCR. (not implemented)
            - 3: Fully automatic page segmentation, but no OSD. (Default)
            - 4: Assume a single column of text of variable sizes.
            - 5: Assume a single uniform block of vertically aligned text.
            - 6: Assume a single uniform block of text.
            - 7: Treat the image as a single text line.
            - 8: Treat the image as a single word.
            - 9: Treat the image as a single word in a circle.
            - 10: Treat the image as a single character.
            - 11:  text. Find as much text as possible in no particular order.
            - 12:  text with OSD.
            - 13:  line. Treat the image as a single text line, bypassing hacks that are Tesseract-specific.

        :param np.array imagem: imagem em forma de array numpy
        :param str lang: idioma para o tesseract
        :param int oem: engine a ser utilizada pelo tesseract
        :param int psm: page segmentation, forma do tesseract interpretar a página
        :returns: texto extraído
        :rtype: str
        """
        config = f'-l {lang} --oem {oem} --psm {psm}'
        resultado = pytesseract.image_to_string(image=imagem, config=config)
        return resultado


from io import BytesIO

from PIL import Image


class DiyImagem:

    @staticmethod
    def jpeg_para_png(bytes_arquivo: bytes) -> bytes:
        """
        Converte o arquivo de JPEG para PNG.

        :params bytes bytes_arquivo: bytes do arquivo
        :returns: bytes em formato PNG
        :rtype: bytes
        """
        buffer = BytesIO(bytes_arquivo)
        imagem = Image.open(fp=buffer)
        buffer = BytesIO()
        imagem.save(buffer, format='png')
        buffer.flush()
        buffer.seek(0)
        return buffer.read()

    @classmethod
    def jpg_para_png(cls, bytes_arquivo: bytes) -> bytes:
        """
        Converte o arquivo de JPG para PNG.

        :params bytes bytes_arquivo: bytes do arquivo
        :returns: bytes em formato PNG
        :rtype: bytes
        """
        return cls.jpeg_para_png(bytes_arquivo)


from io import BytesIO

from pdf2image import convert_from_bytes
from PyPDF2 import PdfFileReader, PdfFileWriter


class DiyPDF:

    @staticmethod
    def bytes_para_pil(bytes_arquivo: bytes, formato_saida: str = 'png') -> list:
        """
        Converte bytes do arquivo pdf para lista de imagens (PIL).

        :param bytes bytes_arquivo: bytes do arquivo
        :param str formato_saida: formato de saída do arquivo
        :returns: uma lista composta por PIL.PngImagePlugin.PngImageFile
        :rtype: list
        """
        return convert_from_bytes(pdf_file=bytes_arquivo, fmt=formato_saida)

    @staticmethod
    def bytes_pil_para_bytes_png(pagina_pil) -> bytes:
        """
        Converte bytes de uma página do arquivo pdf para bytes de um arquivo png.

        :param PngImageFile pagina_pil: bytes da página pdf
        :returns: bytes da página png convertida
        :rtype: bytes
        """
        buffer = BytesIO()
        pagina_pil.save(buffer, format='png')
        buffer.flush()
        buffer.seek(0)
        return buffer.read()

    @staticmethod
    def paginacao_pdf(bytes_arquivo: bytes) -> tuple:
        """
        Separa o pdf em páginas.

        :param bytes bytes_arquivo: bytes do arquivo
        :returns: tupla formada por PyPDF2._writer.PdfFileWriter e a quantidade de páginas
        :rtype: tuple
        """
        leitor = PdfFileReader(stream=BytesIO(bytes_arquivo))
        saida = PdfFileWriter()
        for idx in range(len(leitor.pages)):
            saida.addPage(leitor.getPage(idx))
        return saida, len(leitor.pages)

    @staticmethod
    def extracao_de_texto(pdf: PdfFileWriter, num_paginas: int, separador: str = '\n') -> str:
        """
        Realiza a extração do texto do pdf.

        :param PdfFileWriter pdf: wrapper do pdf
        :param int num_paginas: quantidade de páginas do pdf
        :return: texto do pdf
        :rtype: str
        """
        saida = []
        for idx in range(num_paginas):
            _ = pdf.getPage(idx).extract_text()
            saida.append(_)
        return f'{separador}'.join(saida)
