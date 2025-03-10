package document.parser;

import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;
import org.apache.poi.hwpf.HWPFDocument;
import org.apache.poi.hwpf.extractor.WordExtractor;
import org.apache.poi.xwpf.extractor.XWPFWordExtractor;
import org.apache.poi.xwpf.usermodel.XWPFDocument;

import java.io.ByteArrayInputStream;
import java.io.IOException;

public class DocumentParser {

    public static String extractText(byte[] fileData, String format) throws IOException {
        String extractedText = "";
        switch (format.toLowerCase()) {
            case "doc":
                extractedText = extractTextFromDoc(fileData);
                break;
            case "docx":
                extractedText = extractTextFromDocx(fileData);
                break;
            case "pdf":
                extractedText = extractTextFromPdf(fileData);
                break;
            default:
                throw new IllegalArgumentException("Unsupported file format: " + format);
        }
        return extractedText;
    }

    private static String extractTextFromDoc(byte[] fileData) throws IOException {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(fileData);
             HWPFDocument document = new HWPFDocument(bis);
             WordExtractor extractor = new WordExtractor(document)) {
            return extractor.getText();
        }
    }

    private static String extractTextFromDocx(byte[] fileData) throws IOException {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(fileData);
             XWPFDocument document = new XWPFDocument(bis);
             XWPFWordExtractor extractor = new XWPFWordExtractor(document)) {
            return extractor.getText();
        }
    }

    private static String extractTextFromPdf(byte[] fileData) throws IOException {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(fileData);
             PDDocument document = PDDocument.load(bis)) {
            PDFTextStripper pdfStripper = new PDFTextStripper();
            return pdfStripper.getText(document);
        }
    }
}