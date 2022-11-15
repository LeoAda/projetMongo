# Projet Mongo Isen Velo

**Setup**
---

1. Install python requirement 
   ```sh
    pip install -r requirement.txt
   ```
2. Create .env file
   * Mac or linux
   ```sh 
    touch .env
    ```
   * Windows
   ```sh
    type nul > .env
    ```
3. Add following value to .env file for MongoDB Atlas
    ```sh
    MONGO_USERNAME = juniauser
    MONGO_PASSWORD = juniamdp
    MONGO_CLUSTER_NAME = cluster
    MONGO_DATABASE_NAME = fpruq9w
    ```