package tietokanta;

import java.sql.*;
import java.text.ParseException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Scanner;

/**
 *
 * @author Eetu
 */
public class Tietokanta {

    public Tietokanta() throws SQLException {

    }

    /**
     * @throws java.sql.SQLException
     */
    public static void luoTietokanta() throws SQLException { //Luodaan tietokanta, jossa on Asiakkaat, Paikat, Paketit ja Tapahtumat
        Statement s;
        try (Connection db = DriverManager.getConnection("jdbc:sqlite:tietokanta.db")) {

            s = db.createStatement();
            try {

                s.execute("CREATE TABLE Asiakkaat(id INTEGER PRIMARY KEY, "
                        + "nimi TEXT UNIQUE)");
                s.execute("CREATE TABLE Paikat(id INTEGER PRIMARY KEY, "
                        + "paikka TEXT UNIQUE)");
                s.execute("CREATE TABLE Paketit(id INTEGER PRIMARY KEY, "
                        + "seurantakoodi TEXT UNIQUE, nimi REFERENCES Asiakkaat)");
                s.execute("CREATE TABLE Tapahtumat(id INTEGER PRIMARY KEY, "
                        + "seurantakoodi REFERENCES Paketit, kuvaus TEXT, "
                        + "paikka REFERENCES Paikat, paivamaara TEXT)");

                System.out.println("Tietokanta luotu.");
            } catch (SQLException e) {
                System.out.println("Tietokanta on jo olemassa.");
            }
        }
        s.close();
    }

    public static void lisaaPaikka() throws SQLException { //Lisätään tietokantaan paikka, mikäli samannimistä paikkaa ei jo löydy
        Statement s;
        try (Connection db = DriverManager.getConnection("jdbc:sqlite:tietokanta.db")) {
            s = db.createStatement();
            Scanner lukija = new Scanner(System.in);

            System.out.println("Syotä lisättävän paikan nimi: ");
            String luettu = String.valueOf(lukija.nextLine());

            try {
                s.execute("INSERT INTO Paikat(paikka) VALUES ('" + luettu + "')");
                System.out.println("Lisättiin paikka tietokantaan.");
            } catch (SQLException e) {
                System.out.println("Paikka on jo tietokannassa.");
            }
            s.close();
        }
    }

    public static void lisaaAsiakas() throws SQLException { //Lisätään asiakas tietokantaan, mikäli samannimistä asiakasta ei jo löydy
        Statement s;
        try (Connection db = DriverManager.getConnection("jdbc:sqlite:tietokanta.db")) {
            s = db.createStatement();
            Scanner lukija = new Scanner(System.in);

            System.out.println("Syötä lisättävän asiakkaan nimi: ");
            String luettu = String.valueOf(lukija.nextLine());

            PreparedStatement p = db.prepareStatement("SELECT COUNT(Asiakkaat.nimi) "
                    + "AS SUMMA FROM Asiakkaat WHERE Asiakkaat.nimi = ?");
            p.setString(1, luettu);

            ResultSet r = p.executeQuery();
            try {
                if (r.getInt("SUMMA") == 0) {
                    s.execute("INSERT INTO Asiakkaat(nimi) VALUES ('" + luettu + "')");
                    System.out.println("Lisattiin asiakas nimelta " + luettu + " tietokantaan.");
                } else {
                    System.out.println("Asiakas on jo tietokannassa.");
                }
            } catch (SQLException e) {
                System.out.println("Asiakas on jo tietokannassa.");
            }
        }
        s.close();
    }

    public static void lisaaPaketti() throws SQLException { //Lisätään tietokantaan paketti, sekä asiakkaan nimi ja seurantakoodi paketille
        Statement s;
        try (Connection db = DriverManager.getConnection("jdbc:sqlite:tietokanta.db")) {
            s = db.createStatement();
            Scanner lukija = new Scanner(System.in);

            System.out.println("Syotä paketin seurantakoodi: ");
            String seurantakoodi = String.valueOf(lukija.nextLine());

            System.out.println("Syötä asiakkaan nimi: ");
            String asiakas = String.valueOf(lukija.nextLine());

            PreparedStatement p = db.prepareStatement("SELECT nimi "
                    + "FROM Asiakkaat WHERE nimi=?");
            p.setString(1, asiakas);

            ResultSet r = p.executeQuery();
            try {
                if (r.next()) {
                    s.execute("INSERT INTO Paketit (nimi,seurantakoodi) "
                            + "VALUES ('" + asiakas + "','" + seurantakoodi + "')");
                    System.out.println("Paketti lisätty.");
                } else {
                    System.out.println("Asiakasta ei löytynyt tietokannasta.");
                }
            } catch (SQLException e) {
                System.out.println("Paketin lisäämisessä tapahtui virhe.");
            }
        }
        s.close();
    }

    public static void lisaaTapahtuma() throws SQLException { //Lisätään tapahtuma ja paikka seurantakoodille
        Statement s;
        try (Connection db = DriverManager.getConnection("jdbc:sqlite:tietokanta.db")) {
            s = db.createStatement();
            Scanner lukija = new Scanner(System.in);

            LocalDateTime paiva = LocalDateTime.now();
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd-MM-yyyy "
                    + "HH:mm");
            String paivays = paiva.format(formatter);

            System.out.println("Syotä paketin seurantakoodi: ");
            String seurantakoodi = String.valueOf(lukija.nextLine());
            System.out.println("Syötä tapahtuman paikka: ");
            String tapahtumapaikka = String.valueOf(lukija.nextLine());
            System.out.println("Syötä kuvaus: ");
            String kuvaus = String.valueOf(lukija.nextLine());

            PreparedStatement p = db.prepareStatement("SELECT Paketit.seurantakoodi,"
                    + "Paikat.paikka FROM Paketit, Paikat WHERE seurantakoodi = ?"
                    + " AND paikka = ?");
            p.setString(1, seurantakoodi);
            p.setString(2, tapahtumapaikka);

            ResultSet r = p.executeQuery();
            try {
                if (r.next()) {
                    s.execute("INSERT INTO Tapahtumat(kuvaus, paikka, paivamaara, "
                            + "seurantakoodi) VALUES ('" + kuvaus + "','" + tapahtumapaikka
                            + "','" + paivays + "','" + seurantakoodi + "')");
                    System.out.println("Tapahtuma lisätty.");
                } else {
                    System.out.println("Paikkaa tai seurantakoodia ei löydy.");
                }
            } catch (SQLException e) {
                System.out.println("Tapahtuman lisäämisessä tapahtui virhe.");
            }
        }
        s.close();
    }

    public static void haePakettia() throws SQLException, ParseException { //Haetaan pakettia seurantakoodilla tietokannasta
        Statement s;
        try (Connection db = DriverManager.getConnection("jdbc:sqlite:tietokanta.db")) {
            s = db.createStatement();
            Scanner lukija = new Scanner(System.in);

            System.out.println("Syotä seurantakoodi: ");
            String seurantakoodi = String.valueOf(lukija.nextLine());

            PreparedStatement p = db.prepareStatement("SELECT Tapahtumat.paivamaara,"
                    + "Tapahtumat.kuvaus, Tapahtumat.paikka "
                    + "FROM Paketit, Tapahtumat WHERE Tapahtumat.seurantakoodi = "
                    + "Paketit.seurantakoodi AND Paketit.seurantakoodi = ?");
            p.setString(1, seurantakoodi);

            ResultSet r = p.executeQuery();

            if (!r.isBeforeFirst()) {
                System.out.println("Seurantakoodilla ei löydy pakettia.");
            }
            while (r.next()) {
                System.out.println(r.getString("paivamaara") + ", " + r.getString("paikka")
                        + ", " + r.getString("kuvaus"));
            }
        }
        s.close();
    }

    public static void haeAsiakkaanPakettia() throws SQLException { //Haetaan pakettia tietokannasta asiakkaan nimellä
        Statement s;
        try (Connection db = DriverManager.getConnection("jdbc:sqlite:tietokanta.db")) {
            s = db.createStatement();
            Scanner lukija = new Scanner(System.in);

            System.out.println("Syötä asiakkaan nimi: ");
            String asiakas = String.valueOf(lukija.nextLine());

            PreparedStatement p = db.prepareStatement("SELECT Paketit.seurantakoodi, "
                    + "COUNT (Paketit.seurantakoodi) AS SUMMA "
                    + "FROM Asiakkaat, Tapahtumat, "
                    + "Paketit WHERE Asiakkaat.nimi = ? AND Tapahtumat.seurantakoodi "
                    + "= Paketit.seurantakoodi AND Paketit.nimi = Asiakkaat.nimi "
                    + "GROUP BY Paketit.seurantakoodi");
            p.setString(1, asiakas);

            ResultSet r = p.executeQuery();

            while (r.next()) {
                if (r.getString("seurantakoodi") != null) {
                    System.out.println(r.getString("seurantakoodi") + ", "
                            + r.getInt("SUMMA") + " tapahtumaa.");
                } else {
                    System.out.println("Asiakkaan nimellä ei löydy paketteja.");
                }
            }
        }
        s.close();
    }

    public static void haeTapahtumatPaivamaaralla() throws SQLException { //Haetaan tapahtumat tietokannasta tietyllä päivämäärällä
        Statement s;
        try (Connection db = DriverManager.getConnection("jdbc:sqlite:tietokanta.db")) {
            s = db.createStatement();
            Scanner lukija = new Scanner(System.in);

            System.out.println("Syötä paikan nimi: ");
            String paikka = String.valueOf(lukija.nextLine());
            System.out.println("Syötä päivämäärä (dd-mm-yyyy");
            String paivamaara = String.valueOf(lukija.nextLine());

            PreparedStatement p = db.prepareStatement("SELECT COUNT(Tapahtumat.id) AS"
                    + " SUMMA FROM Tapahtumat WHERE Tapahtumat.paivamaara like ? "
                    + "AND Tapahtumat. paikka = ?");
            p.setString(1, paivamaara + "%");
            p.setString(2, paikka);

            ResultSet r = p.executeQuery();

            try {
                if (r.next()) {
                    if (r.getInt("SUMMA") == 0) {
                        System.out.println("Tapahtumia ei löydy.");
                    } else {
                        System.out.println("Tapahtumien määrä: " + r.getInt("SUMMA"));
                    }
                }
            } catch (SQLException e) {
                System.out.println("Tapahtumia ei löydy.");
            }
        }
        s.close();
    }
}
