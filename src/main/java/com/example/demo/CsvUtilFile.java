package com.example.demo;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class CsvUtilFile {

    public PlayerRepository playerRepository;

    public CsvUtilFile(PlayerRepository playerRepository) {
        this.playerRepository=playerRepository;
    }

    public List<Player> getPlayers(){
        List<Player> list=playerRepository.findAll();
        return list;
    }

    private CsvUtilFile(){}

}
