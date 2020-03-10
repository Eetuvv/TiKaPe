/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package tietokanta;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.util.Random;
import java.util.Scanner;

/**
 *
 * @author Eetu
 */
public class Tietokantasovellus extends Tietokanta {

    public Tietokantasovellus() throws SQLException {

    }

    public static void main(String[] args) throws SQLException, ParseException {
        Scanner lukija = new Scanner(System.in);

        System.out.println("Tervetuloa käyttämään tietokantaa.");
        System.out.println("Komennot: \n"
                + "0 - Lopettaa ohjelman \n"
                + "1 - Luo uuden tietokannan \n"
                + "2 - Lisää uuden paikan tietokantaan \n"
                + "3 - Lisää uuden asiakkaan tietokantaan \n"
                + "4 - Lisää uuden paketin tietokantaan \n"
                + "5 - Lisää uuden tapahtuman tietokantaan \n"
                + "6 - Hakee kaikki paketin tapahtumat seurantakoodin perusteella \n"
                + "7 - Hakee kaikki asiakkaan paketit ja niihin liittyvien "
                + "tapahtumien määrän \n"
                + "8 - Hakee annetusta paikasta tapahtumien määrän tiettynä päivänä \n"
                + "9 - Suorittaa tietokannan tehokkuustestin");

        System.out.println("------------------------------------------");

        while (true) {

            boolean lopetus = false;

            while (true) {
                System.out.println("Syötä komento: ");
                int komento = Integer.valueOf(lukija.nextLine());

                switch (komento) {
                    case 0:
                        System.out.println("Ohjelman suoritus päättyi.");
                        return;
                    case 1:
                        Tietokanta.luoTietokanta();
                        break;
                    case 2:
                        Tietokanta.lisaaPaikka();
                        break;
                    case 3:
                        Tietokanta.lisaaAsiakas();
                        break;
                    case 4:
                        Tietokanta.lisaaPaketti();
                        break;
                    case 5:
                        Tietokanta.lisaaTapahtuma();
                        break;
                    case 6:
                        Tietokanta.haePakettia();
                        break;
                    case 7:
                        Tietokanta.haeAsiakkaanPakettia();
                        break;
                    case 8:
                        Tietokanta.haeTapahtumatPaivamaaralla();
                        break;
                    case 9:
                        //Testataan tietokannan tehokkuutta tehokkuustestillä
                        //Ensimmäinen testi ilman indeksejä ja toinen indekseillä
                        long time1,
                         time2,
                         nanosekunnit;

                        String P = "P";
                        String A = "A";
                        String seurantakoodi = "K";
                        String kuvaus = "ABC";
                        String pvm = "19-02-2020 14.31";

                        System.out.println("Tehokkuustesti alkaa");

                        System.out.println("Lisätään 1000 paikkaa tietokantaan");
                        Statement s;
                        try (Connection db = DriverManager.getConnection("jdbc:sqlite:tietokanta.db")) {
                            s = db.createStatement();
                            s.execute("BEGIN TRANSACTION");
                            time1 = System.nanoTime();
                            PreparedStatement p = db.prepareStatement("INSERT INTO Paikat(paikka) VALUES (?)");
                            for (int i = 0; i < 1000; i++) {
                                p.setString(1, P + i);
                                p.executeUpdate();
                            }
                            time2 = System.nanoTime();
                            double sekunnit = (time2 - time1) / 1000000000;
                            nanosekunnit = time2 - time1;
                            System.out.println("Aikaa paikkojen lisäämiseen kului: " + nanosekunnit + " nanosekuntia (" + sekunnit + "s)");

                            System.out.println("Lisätään 1000 asiakasta tietokantaan");

                            PreparedStatement p2 = db.prepareStatement("INSERT INTO Asiakkaat(nimi) VALUES (?)");
                            time1 = System.nanoTime();

                            for (int i = 0; i < 1000; i++) {
                                p2.setString(1, A + i);
                                p2.executeUpdate();
                            }

                            time2 = System.nanoTime();
                            sekunnit = (time2 - time1) / 1000000000;
                            nanosekunnit = time2 - time1;

                            System.out.println("Aikaa asiakkaiden lisäämiseen kului: " + nanosekunnit + " nanosekuntia (" + sekunnit + "s)");

                            System.out.println("Lisätään 1000 pakettia tietokantaan");

                            PreparedStatement p3 = db.prepareStatement("SELECT nimi "
                                    + "FROM Asiakkaat WHERE nimi=?");

                            time1 = System.nanoTime();
                            for (int i = 0; i < 1000; i++) {
                                p.setString(1, A + i);
                                s.execute("INSERT INTO Paketit (nimi,seurantakoodi) "
                                        + "VALUES ('" + A + i + "','" + seurantakoodi + i + "')");
                            }

                            time2 = System.nanoTime();
                            nanosekunnit = time2 - time1;
                            sekunnit = (time2 - time1) / 1000000000;
                            System.out.println("Aikaa pakettien lisäämiseen kului: " + nanosekunnit + " nanosekuntia (" + sekunnit + "s)");

                            System.out.println("Lisätään miljoona tapahtumaa tietokantaan");

                            PreparedStatement p4 = db.prepareStatement("SELECT Paketit.seurantakoodi,"
                                    + "Paikat.paikka FROM Paketit, Paikat WHERE seurantakoodi = ?"
                                    + " AND paikka = ?");

                            time1 = System.nanoTime();
                            Random random = new Random();
                            for (int i = 0; i < 1000000; i++) {
                                int y = random.nextInt(1000 - 0 + 1) + 0;
                                p4.setString(1, seurantakoodi + y);
                                p4.setString(2, P + y);
                                s.execute("INSERT INTO Tapahtumat(kuvaus, paikka, paivamaara, "
                                        + "seurantakoodi) VALUES ('" + kuvaus + i + "','" + P + y
                                        + "','" + pvm + "','" + seurantakoodi + y + "')");
                            }
                            time2 = System.nanoTime();
                            sekunnit = (time2 - time1) / 1000000000;
                            s.execute("COMMIT");

                            System.out.println("Aikaa tapahtumien lisäämiseen kului: " + sekunnit + "sekuntia");

                            System.out.println("Suoritetaan 1000 kyselyä jossa "
                                    + "kussakin haetaan jonkin asiakkaan pakettien määrä");

                            PreparedStatement p5 = db.prepareStatement("SELECT COUNT(Asiakkaat.nimi) "
                                    + "FROM Asiakkaat,Paketit WHERE Asiakkaat.nimi"
                                    + " = Paketit.nimi AND Asiakkaat.nimi = ?");

                            for (int i = 0; i < 1000; i++) {
                                p5.setString(1, A + i);
                                p5.executeQuery();
                            }

                            s.execute("BEGIN TRANSACTION");

                            time1 = System.nanoTime();
                            for (int i = 0; i < 1000; i++) {
                                p5.executeQuery();
                            }
                            time2 = System.nanoTime();
                            nanosekunnit = time2 - time1;
                            sekunnit = (time2 - time1) / 1000000000;
                            s.execute("COMMIT");

                            System.out.println("Aikaa kyselyyn 1 meni " + nanosekunnit + " nanosekuntia (" + sekunnit + "s)");

                            System.out.println("Suoritetaan tuhat kyselyä joissa kussakin haetaan"
                                    + " johonkin pakettiin liittyvien tapahtumien määrä");

                            PreparedStatement p6 = db.prepareStatement("SELECT COUNT(Tapahtumat.seurantakoodi) "
                                    + "FROM Tapahtumat WHERE seurantakoodi =?");

                            s.execute("BEGIN TRANSACTION");
                            time1 = System.nanoTime();
                            for (int i = 0; i < 1000; i++) {
                                p6.setString(1, seurantakoodi + i);
                                p6.executeQuery();
                            }
                            time2 = System.nanoTime();
                            sekunnit = (time2 - time1) / 1000000000;
                            s.execute("COMMIT");

                            System.out.println("Aikaa kyselyyn 2 meni " + sekunnit + " sekuntia");

                        }
                        s.close();
                        
                        System.out.println("");
                        System.out.println("");
                        System.out.println("Luodaan uusi tietokanta indekseillä");

                        try (Connection db = DriverManager.getConnection("jdbc:sqlite:tietokantatesti.db")) {
                            s = db.createStatement();
                            s.execute("CREATE TABLE Asiakkaat(id INTEGER PRIMARY KEY, "
                                    + "nimi TEXT UNIQUE)");
                            s.execute("CREATE TABLE Paikat(id INTEGER PRIMARY KEY, "
                                    + "paikka TEXT UNIQUE)");
                            s.execute("CREATE TABLE Paketit(id INTEGER PRIMARY KEY, "
                                    + "seurantakoodi TEXT UNIQUE, nimi REFERENCES Asiakkaat)");
                            s.execute("CREATE TABLE Tapahtumat(id INTEGER PRIMARY KEY, "
                                    + "seurantakoodi REFERENCES Paketit, kuvaus TEXT, "
                                    + "paikka REFERENCES Paikat, paivamaara TEXT)");

                            s.execute("CREATE INDEX idx_seurantakoodi ON Tapahtumat (seurantakoodi)");
                            s.execute("CREATE INDEX idx_nimi ON Asiakkaat (nimi)");
                            
                            System.out.println("Testi 2 indekseillä alkaa");

                            s.execute("BEGIN TRANSACTION");
                            time1 = System.nanoTime();
                            PreparedStatement p = db.prepareStatement("INSERT INTO Paikat(paikka) VALUES (?)");
                            for (int i = 0; i < 1000; i++) {
                                p.setString(1, P + i);
                                p.executeUpdate();
                            }
                            time2 = System.nanoTime();
                            nanosekunnit = time2 - time1;
                            double sekunnit = (time2 - time1) / 1000000000;
                            System.out.println("Aikaa paikkojen lisäämiseen kului: " + nanosekunnit + " nanosekuntia (" + sekunnit + "s)");

                            System.out.println("Lisätään 1000 asiakasta tietokantaan");

                            PreparedStatement p2 = db.prepareStatement("INSERT INTO Asiakkaat(nimi) VALUES (?)");
                            time1 = System.nanoTime();

                            for (int i = 0; i < 1000; i++) {
                                p2.setString(1, A + i);
                                p2.executeUpdate();
                            }

                            time2 = System.nanoTime();
                            nanosekunnit = time2 - time1;
                            sekunnit = (time2 - time1) / 1000000000;
                            
                            System.out.println("Aikaa asiakkaiden lisäämiseen kului: " + nanosekunnit + " nanosekuntia (" + sekunnit + "s)");

                            System.out.println("Lisätään 1000 pakettia tietokantaan");

                            PreparedStatement p3 = db.prepareStatement("SELECT nimi "
                                    + "FROM Asiakkaat WHERE nimi=?");

                            time1 = System.nanoTime();
                            for (int i = 0; i < 1000; i++) {
                                p.setString(1, A + i);
                                s.execute("INSERT INTO Paketit (nimi,seurantakoodi) "
                                        + "VALUES ('" + A + i + "','" + seurantakoodi + i + "')");
                            }

                            time2 = System.nanoTime();
                            nanosekunnit = time2 - time1;
                            sekunnit = (time2 - time1) / 1000000000;
                            System.out.println("Aikaa pakettien lisäämiseen kului: " + nanosekunnit + " nanosekuntia (" + sekunnit + "s)");

                            System.out.println("Lisätään miljoona tapahtumaa tietokantaan");

                            PreparedStatement p4 = db.prepareStatement("SELECT Paketit.seurantakoodi,"
                                    + "Paikat.paikka FROM Paketit, Paikat WHERE seurantakoodi = ?"
                                    + " AND paikka = ?");

                            time1 = System.nanoTime();
                            Random random = new Random();
                            for (int i = 0; i < 1000000; i++) {
                                int y = random.nextInt(1000 - 0 + 1) + 0;
                                p4.setString(1, seurantakoodi + y);
                                p4.setString(2, P + y);
                                s.execute("INSERT INTO Tapahtumat(kuvaus, paikka, paivamaara, "
                                        + "seurantakoodi) VALUES ('" + kuvaus + i + "','" + P + y
                                        + "','" + pvm + "','" + seurantakoodi + y + "')");
                            }
                            time2 = System.nanoTime();
                            sekunnit = (time2 - time1) / 1000000000;
                            s.execute("COMMIT");

                            System.out.println("Aikaa tapahtumien lisäämiseen kului: " + sekunnit + "sekuntia");

                            System.out.println("Suoritetaan 1000 kyselyä jossa "
                                    + "kussakin haetaan jonkin asiakkaan pakettien määrä");

                            PreparedStatement p5 = db.prepareStatement("SELECT COUNT(Asiakkaat.nimi) "
                                    + "FROM Asiakkaat,Paketit WHERE Asiakkaat.nimi"
                                    + " = Paketit.nimi AND Asiakkaat.nimi = ?");

                            for (int i = 0; i < 1000; i++) {
                                p5.setString(1, A + i);
                                p5.executeQuery();
                            }

                            s.execute("BEGIN TRANSACTION");

                            time1 = System.nanoTime();
                            for (int i = 0; i < 1000; i++) {
                                p5.executeQuery();
                            }
                            time2 = System.nanoTime();
                            nanosekunnit = time2 - time1;
                            sekunnit = (time2 - time1) / 1000000000;
                            s.execute("COMMIT");

                            System.out.println("Aikaa kyselyyn 1 meni " + nanosekunnit + " nanosekuntia (" + sekunnit + "s)");

                            System.out.println("Suoritetaan tuhat kyselyä joissa kussakin haetaan"
                                    + " johonkin pakettiin liittyvien tapahtumien määrä");

                            PreparedStatement p6 = db.prepareStatement("SELECT COUNT(Tapahtumat.seurantakoodi) "
                                    + "FROM Tapahtumat WHERE seurantakoodi =?");

                            s.execute("BEGIN TRANSACTION");
                            time1 = System.nanoTime();
                            for (int i = 0; i < 1000; i++) {
                                p6.setString(1, seurantakoodi + i);
                                p6.executeQuery();
                            }
                            time2 = System.nanoTime();
                            nanosekunnit = (time2 - time1);
                            sekunnit = (time2 - time1) / 1000000000;
                            s.execute("COMMIT");

                            System.out.println("Aikaa kyselyyn 2 meni " + nanosekunnit + " nanosekuntia (" + sekunnit + "s)");

                        }
                        s.close();
                        break;

                    default:
                        System.out.println("Väärä komento.");
                }

                break;
            }
        }
    }
}
