# 🎓 الدليل التعليمي الشامل لهندسة خطوط أنابيب البيانات (ETL Masterclass)
> **وضع المعلم (Tutor Mode): مُفعّل**  
> مرحباً بك يا بطل البيانات! تم إعداد هذا الدليل ليكون مرجعك التعليمي التفاعلي الشامل. سنشرح هنا كل سطر برمجي، وكل قرار معماري اتخذناه في بناء مشروع **`student_data_pipeline`**، مع طرح أسئلة ذكية في نهاية كل محطة لاختبار استيعابك وتثبيت المعلومة في ذهنك.

---

## 📑 فهرس المحتويات التعليمية
1. [المحطة 1: الفلسفة المعمارية ونمط الأنابيب والمرشحات (Architecture & Pipes-Filters)](#المحطة-1-الفلسفة-المعمارية-ونمط-الأنابيب-والمرشحات)
2. [المحطة 2: طبقة الاستخراج من المصادر غير المتجانسة (Extraction Layer)](#المحطة-2-طبقة-الاستخراج-من-المصادر-غير-المتجانسة)
3. [المحطة 3: تنظيف النصوص وتوحيدها (Text Cleaning & Normalization)](#المحطة-3-تنظيف-النصوص-وتوحيدها)
4. [المحطة 4: تكامل ودمج البيانات متعددة المصادر (Data Integration)](#المحطة-4-تكامل-ودمج-البيانات-متعددة-المصادر)
5. [المحطة 5: التحويل وهندسة الخصائص المشتقة (Transformation & Feature Engineering)](#المحطة-5-التحويل-وهندسة-الخصائص-المشتقة)
6. [المحطة 6: بوابات الجودة وعزل السجلات (Quality Gates & Dead Letter Queue)](#المحطة-6-بوابات-الجودة-وعزل-السجلات)
7. [المحطة 7: المايسترو، نظام التسجيل، والاختبارات (Orchestration, Logging & Testing)](#المحطة-7-المايسترو-نظام-التسجيل-والاختبارات)
8. [بنك الإجابات والحلول النموذجية لأسئلة الفهم](#بنك-الإجابات-والحلول-النموذجية)

---

## المحطة 1: الفلسفة المعمارية ونمط الأنابيب والمرشحات

### 🧠 المفهوم النظري:
في هندسة البيانات الحديثة، لا نكتب كود المشروع في ملف واحد طويل (`spaghetti code`). بل نطبق مبدأ **فصل المسؤوليات (Separation of Concerns)** وفق نمط معمارية **الأنابيب والمرشحات (Pipes and Filters)**:
- **المُرشح (Filter):** دالة أو وحدة برمجية تنفذ عملية معالجة واحدة محددة (مثل: تنظيف، أو تحويل، أو فحص جودة).
- **الأنبوب (Pipe):** قناة نقل البيانات (في حالتنا: كائن `pandas.DataFrame`) التي تنقل المخرجات من مرشح ليكون مدخلاً للمرشح التالي.

```text
[مصادر البيانات] ──(استخراج)──> [تنظيف] ──(دمج)──> [تحويل] ──(فحص جودة)──> [تخزين المخرجات]
```

### 📁 هيكلية المجلدات ولماذا صُممت هكذا؟
- `app/sources/`: وظيفتها الحصرية هي **القراءة** فقط (Extract) دون التدخل في تعديل البيانات.
- `app/transformation/`: مسؤولة عن **تعديل وتشكيل** البيانات (Clean, Transform, Integrate).
- `app/validation/`: هي بمثابة **حارس البوابة** (Quality Gate)، تقرر من يدخل ومن يُعزل.
- `app/output/`: وظيفتها **الكتابة والتخزين** فقط (Load).
- `app/utils/`: أدوات مساعدة مشتركة مثل الـ `logger.py`.

---

### ❓ سؤال اختبار الفهم (1):
> **سؤال:** إذا طلب منك مدير المشروع مستقبلاً استخراج بيانات الطلاب من ملفات `JSON` جديدة، ما هو المجلد الوحيد الذي ستقوم بإنشاء كود جديد داخله دون المساس بباقي المنظومة؟  
> *(فكر في الإجابة، وستجد الحل في نهاية الملف).*

---

## المحطة 2: طبقة الاستخراج من المصادر غير المتجانسة

في هذه الطبقة قمنا باستخراج البيانات من 3 عوالم مختلفة: ملف مسطح، قاعدة بيانات علائقية، وخادم ويب (API).

---

### 1. استخراج ملف CSV (`app/sources/csv_source.py`)

#### 🔍 الكود وشرحه:
```python
def load_csv_source(file_path: str | Path) -> pd.DataFrame:
    path = Path(file_path)
    # 1. التأكد البرمجي الدفاعي من وجود الملف قبل محاولة فتحه
    if not path.exists():
        logger.error(f"CSV file not found at: {path}")
        raise FileNotFoundError(f"File not found: {path}")

    try:
        # 2. قراءة الملف عبر pandas
        df = pd.read_csv(path)
        return df
    except pd.errors.EmptyDataError:
        # 3. معالجة حالة إذا كان الملف موجوداً ولكنه فارغ تماماً 0 بايت
        logger.warning(f"CSV file at {path} is empty.")
        return pd.DataFrame()
```
- **لماذا استخدمنا `pathlib.Path`؟** لأنها طريقة احترافية في بايثون تعمل بسلاسة عبر جميع أنظمة التشغيل (Windows و Linux) دون مشاكل الشرطة المائلة `\` أو `/`.
- **البرمجة الدفاعية (Defensive Programming):** لا ننتظر انهيار الكود، بل نفحص وجود الملف أولاً، ونعالج حالة الملف الفارغ `EmptyDataError`.

---

### 2. استخراج قاعدة البيانات بـ SQL Join (`app/sources/database_source.py`)

قاعدة البيانات تحتوي على جدولين: `academic_profiles` و `academic_grades`.

#### 🔍 استعلام الـ SQL المدمج:
```sql
SELECT 
    p.student_id,
    p.major,
    p.enrollment_year,
    g.gpa,
    g.total_credits
FROM academic_profiles p
INNER JOIN academic_grades g ON p.student_id = g.student_id
```

#### 💡 لماذا دمجنا عبر الـ SQL داخل قاعدة البيانات مباشرة بدلاً من دمجها في بايثون؟
- **مبدأ دفع الحسابات للمصدر (Push-down Predicate / Compute at Source):** محركات قواعد البيانات (مثل SQLite أو PostgreSQL) مكتوبة بلغة C ومحسنة جداً للقيام بعمليات الـ `JOIN` بشكل أسرع وأقل استهلاكاً لذاكرة الرام مقارنة بجلب الجدولين منفصلين إلى بايثون ثم دمجهما.

---

### 3. استخراج الـ REST API والصمود ضد الأعطال (`app/sources/api_source.py`)

شبكات الإنترنت معرضة دائماً للمشاكل؛ لذلك قمنا ببناء معالج استثنائي صامد (Resilient API Client):

#### 🔍 كود معالجة استثناءات الشبكة والـ JSON:
```python
try:
    with urllib.request.urlopen(req, timeout=timeout) as response:
        raw_data = response.read().decode("utf-8")

    # 1. فحص تلف صيغة الـ JSON (Invalid JSON Error)
    try:
        payload = json.loads(raw_data)
    except (json.JSONDecodeError, ValueError) as json_err:
        logger.error(f"[Invalid JSON Error]: {json_err}")
        return get_mock_attendance_data()

except (urllib.error.URLError, ConnectionError) as conn_err:
    # 2. فحص فشل الاتصال (Connection Error)
    logger.error(f"[Connection Error]: {conn_err}")
    return get_mock_attendance_data()

except (socket.timeout, TimeoutError) as timeout_err:
    # 3. فحص بطء الخادم وانتهاء المهلة (Timeout Error)
    logger.error(f"[Timeout Error]: {timeout_err}")
    return get_mock_attendance_data()
```
- **آلية التعافي (Fallback Mechanism):** إذا انقطع الإنترنت أو كان الخادم معطلاً، لا يتوقف خط الأنابيب، بل يستدعي دالة `get_mock_attendance_data()` لتزويده ببيانات تجريبية تحافظ على استمرار تدفق البيانات (High Availability).

---

### ❓ سؤال اختبار الفهم (2):
> **سؤال:** ما هو الخطر المحتمل إذا قمنا باستدعاء REST API خارجي بدون تحديد معامل المهلة الزمنية `timeout`؟  
> *(أ) سيعود الكود بخطأ SyntaxError.*  
> *(ب) قد يعلق خط الأنابيب للأبد (Hang/Freeze) إذا توقف خادم الـ API عن الاستجابة.*  
> *(ج) سيتم مسح بيانات قاعدة البيانات.*

---

## المحطة 3: تنظيف النصوص وتوحيدها

ملف `app/transformation/cleaner.py` مسؤول عن جعل البيانات الفوضوية بيانات نظيفة وموحدة.

### 🔍 تفكيك كود التنظيف سطر بسطر:

#### 1. توحيد أسماء الأعمدة:
```python
cleaned.columns = (
    cleaned.columns.astype(str)
    .str.strip()                # إزالة أي مسافات حول الاسم مثل " Student ID "
    .str.lower()                # تحويل كل الأحرف لصغيرة
    .str.replace(" ", "_")      # استبدال المسافات بشرطة سفلية
    .str.replace("-", "_")
)
```
- **الهدف:** توحيد مسميات الأعمدة لتكون بصيغة `lower_snake_case` القياسية (مثل `student_id`).

#### 2. توحيد وحل تباين أسماء المدن (City Normalization):
قام المدخل البشري بكتابة اسم المدينة بطرق مختلفة: `" cairo "`, `"CAIRO"`, `"new york"`, `"RIYADH"`.
```python
CITY_NORMALIZATION_MAP = {
    "cairo": "Cairo",
    "alex": "Alexandria",
    "alexandria": "Alexandria",
    "riyadh": "Riyadh",
    "new york": "New York",
}

def _normalize_city(val):
    if pd.isna(val): return pd.NA
    s = str(val).strip()
    return CITY_NORMALIZATION_MAP.get(s.lower(), s.title())
```
- نقوم بتحويل النص إلى أحرف صغيرة للبحث في القاموس، فإذا وجدناه استبدلناه بالاسم الرسمي، وإلا نطبق `s.title()` لتكبير الحرف الأول من كل كلمة تلقائياً.

#### 3. إزالة التكرارات:
```python
cleaned = cleaned.drop_duplicates()
```
- تحذف السجلات التي تتطابق في جميع القيم تماماً لمنع مضاعفة الحسابات.

---

### ❓ سؤال اختبار الفهم (3):
> **سؤال:** إذا كانت قيمة المدينة في أحد السجلات هي `"  riyadh  "`، ماذا ستكون قيمتها بعد المرور بدالة `clean_student_data`؟

---

## المحطة 4: تكامل ودمج البيانات متعددة المصادر

في `app/transformation/integration.py`، نقوم بدمج مخرجات المصادر الثلاثة:
1. `df_csv`: البيانات الديموغرافية (`name`, `city`, `age`, `email`).
2. `df_db`: البيانات الأكاديمية (`major`, `gpa`, `total_credits`).
3. `df_api`: بيانات الحضور (`attendance_rate`).

### 🔍 كود الدمج الذكي:
```python
def integrate_sources(sources: List[pd.DataFrame], join_key: str = "student_id", how: str = "outer"):
    ...
    integrated = pd.merge(integrated, next_df, on=join_key, how=how)
```

### 💡 لماذا اخترنا `how="outer"` وليس `inner`؟
- في الـ **Inner Join**: إذا وُجد طالب مسجل في ملف الـ CSV لكن لم ترصد له درجات بعد في الـ DB أو لم تسجل له نسبة حضور في الـ API، سيتم **حذفه بالكامل** وضياع سجله!
- في الـ **Outer Join**: نحافظ على جميع السجلات القادمة من كافة المصادر، ونضع `NaN` أمام الحقول غير المتوفرة، ونترك القرار النهائي لحارس بوابات الجودة (Quality Validator).

---

### ❓ سؤال اختبار الفهم (4):
> **سؤال:** لديك طالب يحمل الرقم `111`، بياناته موجودة في ملف الـ CSV، لكنه غير موجود في قاعدة بيانات الدرجات.  
> إذا دمجنا باستخدام `how="inner"`، هل سيظهر هذا الطالب في الناتج المدمج؟ وماذا لو دمجنا بـ `how="outer"`؟

---

## المحطة 5: التحويل وهندسة الخصائص المشتقة

في `app/transformation/transformer.py`، نحوّل البيانات الخام إلى مؤشرات ذكية للأعمال (Feature Engineering).

### 🔍 1. تصحيح أنواع البيانات (Type Casting):
```python
transformed["student_id"] = pd.to_numeric(transformed["student_id"], errors="coerce").astype("Int64")
transformed["gpa"] = pd.to_numeric(transformed["gpa"], errors="coerce")
```
- **لماذا استخدمنا `Int64` بحرف كبير (Capital I) بدلاً من `int64` العادي؟**
  في بايثون، النوع `int` التقليدي لا يقبل القيم المفقودة `NaN` (سينهار الكود إذا وجد قيمة فارغة). أما `Int64` الخاص بـ pandas فهو يقبل القيم الفارغة دون أي خطأ (Nullable Integer).

---

### 🔍 2. اشتقاق مستوى الأداء (`performance_level`) من الـ GPA:
وفقاً للنطاق الأكاديمي لمعدل الـ GPA من $0.0$ إلى $4.0$:
$$\text{Performance Level} = 
\begin{cases} 
\text{Excellent} & \text{if } \text{GPA} \ge 3.7 \\
\text{Very Good} & \text{if } 3.0 \le \text{GPA} < 3.7 \\
\text{Good} & \text{if } 2.5 \le \text{GPA} < 3.0 \\
\text{Satisfactory} & \text{if } 2.0 \le \text{GPA} < 2.5 \\
\text{Academic Probation} & \text{if } \text{GPA} < 2.0 
\end{cases}$$

```python
def _compute_performance_level(gpa):
    if pd.isna(gpa): return "Not Available"
    val = float(gpa)
    if val >= 3.7: return "Excellent"
    if val >= 3.0: return "Very Good"
    if val >= 2.5: return "Good"
    if val >= 2.0: return "Satisfactory"
    return "Academic Probation"
```

---

### 🔍 3. اشتقاق حالة الحضور (`attendance_status`):
وفقاً للنسبة المئوية للحضور من $0\%$ إلى $100\%$:
$$\text{Attendance Status} = 
\begin{cases} 
\text{Regular} & \text{if } \text{Rate} \ge 85\% \\
\text{Needs Improvement} & \text{if } 75\% \le \text{Rate} < 85\% \\
\text{Critical Warning} & \text{if } \text{Rate} < 75\% 
\end{cases}$$

---

### ❓ سؤال اختبار الفهم (5):
> **سؤال:** طالب حصل على معدل تراكمي $\text{GPA} = 1.85$ ونسبة حضور $92\%$، ما هي القيم التي سيتم توليدها له في عمودي:  
> `performance_level` و `attendance_status`؟

---

## المحطة 6: بوابات الجودة وعزل السجلات

في `app/validation/quality.py`، نطبق فلسفة **عدم الثقة المطلقة في البيانات (Zero Trust Data Quality)**.

### 🛡️ قواعد الجودة الصارمة المطبقة:
1. `student_id`: يجب أن يكون موجوداً، موجباً، وغير مكرر.
2. `age`: يجب أن يقع ضمن النطاق المنطقي لطلاب الجامعة: $16 \le \text{age} \le 80$.
3. `gpa`: يجب ألا يقل عن $0.0$ وألا يزيد عن $4.0$.
4. `attendance_rate`: يجب أن تكون نسبة مئوية صحيحة بين $0.0$ و $100.0$.
5. `email`: يجب أن يحتوي على علامة `@` واسم نطاق صحيح.

---

### 🔍 استراتيجية عزل السجلات المرفوضة (Dead Letter Queue Strategy):
بدلاً من حذف السجلات المعطوبة (Dropping)، يقوم الكود بجمع كل أسباب الرفض داخل عمود مخصص باسم **`error_reason`** وتخزينها في ملف منفصل:

```python
for idx, row in df_copy.iterrows():
    errors = []
    if "age" in row and (row["age"] < 16 or row["age"] > 80):
        errors.append(f"Age out of bounds [16-80]: {row['age']}")
    if "gpa" in row and (row["gpa"] < 0.0 or row["gpa"] > 4.0):
        errors.append(f"GPA out of bounds [0.0-4.0]: {row['gpa']}")
    ...
    if errors:
        reasons_list.append("; ".join(errors)) # دمج كافة الأخطاء للسجل الواحد
    else:
        reasons_list.append(None)

# فصل السجلات المقبولة عن المرفوضة
rejected_records = df_copy[df_copy["error_reason"].notna()]
valid_records = df_copy[df_copy["error_reason"].isna()].drop(columns=["error_reason"])
```

#### 📊 مخرجات هذه العملية:
- **`data/processed/final_dataset.csv`**: البيانات المقبولة فقط (الصالحة للتحليل والـ AI).
- **`data/rejected/rejected_records.csv`**: البيانات المرفوضة لمعرفة الخلل وإصلاحه مع مسؤولي المصادر.

---

### ❓ سؤال اختبار الفهم (6):
> **سؤال:** إذا كان لسجل معين خطأين: العمر $12$ والمعدل $4.5$:  
> 1. إلى أي ملف سيتجه هذا السجل؟  
> 2. ماذا ستكون قيمة عمود `error_reason` له؟

---

## المحطة 7: المايسترو، نظام التسجيل، والاختبارات

### 1. المايسترو (`main.py`)
هو العقل المدبر الذي ينسق خطوات خط الأنابيب بترتيب منطقي دقيق:
1. `Extract`: استدعاء `load_csv_source`, `load_database_source`, `fetch_api_source`.
2. `Clean`: استدعاء `clean_student_data`.
3. `Integrate`: استدعاء `integrate_sources`.
4. `Transform`: استدعاء `transform_student_data`.
5. `Validate`: استدعاء `validate_student_records`.
6. `Load`: استدعاء `write_csv_output` لكتابة الملف النهائي وملف المرفوضات.

---

### 2. نظام التسجيل المزدوج (`app/utils/logger.py`)
يقوم بتسجيل مسار تدقيق كامل (Audit Trail):
- يطبع على الشاشة للمطور (`sys.stdout`).
- يكتب في ملف دائم `logs/pipeline.log`.
- كل سطر يحتوي على: `[التاريخ والوقت] [المستوى INFO/ERROR] [اسم المكون] - [الرسالة]`.

---

### 3. الاختبارات الآلية بـ pytest (`tests/test_pipeline.py`)
لا نعتمد على الحظ! قمنا بكتابة 10 اختبارات تغطي:
- اختبار قراءة الـ CSV والـ Database والـ API.
- اختبار تنظيف أسماء المدن وإزالة المسافات.
- اختبار دقة حساب `performance_level` و `attendance_status`.
- اختبار فحص قواعد الجودة وقدرتها على رصد الشذوذ وعزله في `error_reason`.

تشغيل الاختبارات:
```bash
python -m pytest -v
```

---

## 🏆 بنك الإجابات والحلول النموذجية

تهانينا على وصولك إلى هنا! دعنا نتحقق من إجاباتك على أسئلة الفهم:

### حل سؤال (1):
> **الإجابة:** المجلد الوحيد هو **`app/sources/`** (عبر إنشاء ملف مثل `json_source.py`). هذا هو جمال معمارية الأنابيب والمرشحات؛ يمكنك تغيير طريقة جلب البيانات دون لمس كود التنظيف أو فحص الجودة!

### حل سؤال (2):
> **الإجابة الصحيحة:** **(ب)** قد يعلق خط الأنابيب للأبد (Freeze) بانتظار استجابة قد لا تأتي أبداً، مما يوقف مهام النظام بالكامل؛ لذلك تحديد `timeout` هو قاعدة ذهبية في بيئات الإنتاج.

### حل سؤال (3):
> **الإجابة:** ستصبح **`Riyadh`** بحرف كابيتال ومن دون أي مسافات زائدة؛ لأن الدالة قامت بعمل `.strip()` لإزالة المسافات، ثم طبقت قاموس التوحيد `CITY_NORMALIZATION_MAP`.

### حل سؤال (4):
> **الإجابة:** 
> - في `inner`: **لن يظهر** وسيتم حذفه لأن شرط التطابق لم يتحقق في قاعدة البيانات.
> - في `outer`: **سيظهر** وتكون بيانات درجاته فارغة `NaN`، مما يسمح بحفظ بياناته وفحصها في طبقة الجودة.

### حل سؤال (5):
> **الإجابة:**
> - `performance_level` ستكون: **`Academic Probation`** (لأن المعدل أقل من 2.0).
> - `attendance_status` ستكون: **`Regular`** (لأن الحضور $\ge 85\%$).

### حل سؤال (6):
> **الإجابة:**
> 1. سيتجه إلى ملف المرفوضات: **`data/rejected/rejected_records.csv`**.
> 2. قيمة `error_reason` ستكون:  
>    `Age out of bounds [16-80]: 12.0; GPA out of bounds [0.0-4.0]: 4.5`

---
> 💡 **نصيحة المعلم للمستقبل:**  
> هذا المشروع يمثل الأساس المعياري لأي مهندس بيانات محترف. يمكنك الآن استخدام هذه المعمارية كقالب (Template) لبناء أي خط أنابيب بيانات حقيقي في مشاريعك القادمة.

